#include "prepared_meta.h"
#include "dispatch_by_type.h"
#include "memory_helpers.h"

#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt/core/misc/bit_packing.h>

namespace NYT::NNewTableClient {

using TSegmentMetas = TRange<const NProto::TSegmentMeta*>;

////////////////////////////////////////////////////////////////////////////////

bool IsDirect(int type)
{
    // DirectRle/DirectSparse: 2,  DirectDense: 3
    return type == 2 || type == 3;
}

bool IsDense(int type)
{
    // DictionaryDense: 1, DirectDense: 3
    return type == 1 || type == 3;
}

const ui64* InitCompressedVectorHeader(const ui64* ptr, ui32* size, ui8* width)
{
    TCompressedVectorView view(ptr);
    *size = view.GetSize();
    *width = view.GetWidth();
    ptr += view.GetSizeInWords();
    return ptr;
}

void TMetaBase::InitFromProto(const NProto::TSegmentMeta& meta)
{
    DataOffset = meta.offset();
    RowCount = meta.row_count();
    ChunkRowCount = meta.chunk_row_count();
}

void TTimestampMeta::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    TMetaBase::InitFromProto(meta);

    const auto& timestampMeta = meta.GetExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta);
    BaseTimestamp = timestampMeta.min_timestamp();
    ExpectedDeletesPerRow = timestampMeta.expected_deletes_per_row();
    ExpectedWritesPerRow = timestampMeta.expected_writes_per_row();

    if (ptr) {
        ptr = InitCompressedVectorHeader(ptr, &TimestampsDictSize, &TimestampsDictWidth);
        ptr = InitCompressedVectorHeader(ptr, &WriteTimestampSize, &WriteTimestampWidth);
        ptr = InitCompressedVectorHeader(ptr, &DeleteTimestampSize, &DeleteTimestampWidth);
        ptr = InitCompressedVectorHeader(ptr, &WriteOffsetDiffsSize, &WriteOffsetDiffsWidth);
        ptr = InitCompressedVectorHeader(ptr, &DeleteOffsetDiffsSize, &DeleteOffsetDiffsWidth);

        YT_VERIFY(WriteOffsetDiffsSize == RowCount);
        YT_VERIFY(WriteOffsetDiffsSize == RowCount);
    }
}

const ui64* TIntegerMeta::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    const auto& integerMeta = meta.GetExtension(NProto::TIntegerSegmentMeta::integer_segment_meta);
    BaseValue = integerMeta.min_value();

    Direct = IsDirect(meta.type());

    if (ptr) {
        ptr = InitCompressedVectorHeader(ptr, &ValuesSize, &ValuesWidth);
        if (Direct) {
            ptr += GetBitmapSize(ValuesSize);
        } else {
            ptr = InitCompressedVectorHeader(ptr, &IdsSize, &IdsWidth);
        }
    }
    return ptr;
}

void TBlobMeta::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    const auto& stringMeta = meta.GetExtension(NProto::TStringSegmentMeta::string_segment_meta);
    ExpectedLength = stringMeta.expected_length();

    Direct = IsDirect(meta.type());

    if (ptr) {
        if (Direct) {
            ptr = InitCompressedVectorHeader(ptr, &OffsetsSize, &OffsetsWidth);
            ptr += GetBitmapSize(OffsetsSize);
        } else {
            ptr = InitCompressedVectorHeader(ptr, &IdsSize, &IdsWidth);
            ptr = InitCompressedVectorHeader(ptr, &OffsetsSize, &OffsetsWidth);
        }
    }
}

const ui64* TDataMeta<EValueType::Boolean>::InitFromProto(const NProto::TSegmentMeta& /*meta*/, const ui64* ptr)
{
    if (ptr) {
        ui64 count = *ptr++;
        ptr += GetBitmapSize(count);
        ptr += GetBitmapSize(count);
    }

    return ptr;
}

const ui64* TDataMeta<EValueType::Double>::InitFromProto(const NProto::TSegmentMeta& /*meta*/, const ui64* ptr)
{
    if (ptr) {
        ui64 count = *ptr++;
        ptr += count;
        ptr += GetBitmapSize(count);
    }

    return ptr;
}

const ui64* TMultiValueIndexMeta::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr, bool aggregate)
{
    TMetaBase::InitFromProto(meta);

    bool dense = meta.HasExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
    if (dense) {
        const auto& denseVersionedMeta = meta.GetExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
        ExpectedPerRow = denseVersionedMeta.expected_values_per_row();
    } else {
        ExpectedPerRow = -1;
    }

    if (ptr) {
        ptr = InitCompressedVectorHeader(ptr, &OffsetsSize, &OffsetsWidth);
        ptr = InitCompressedVectorHeader(ptr, &WriteTimestampIdsSize, &WriteTimestampIdsWidth);
        if (aggregate) {
            ptr += GetBitmapSize(WriteTimestampIdsSize);
        }
    }

    return ptr;
}

const ui64* TKeyIndexMeta::InitFromProto(const NProto::TSegmentMeta& meta, EValueType type, const ui64* ptr)
{
    TMetaBase::InitFromProto(meta);
    Dense = type == EValueType::Double || type == EValueType::Boolean || IsDense(meta.type());

    if (ptr) {
        if (!Dense) {
            ptr = InitCompressedVectorHeader(ptr, &RowIndexesSize, &RowIndexesWidth);
        }
    }

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

struct TPrepareResult
{
    std::vector<ui32> BlockIds;
    std::vector<ui32> SegmentPivots;
    TSharedRef Meta;
};

template <class TMeta>
static TPrepareResult DoPrepare(TSegmentMetas metas, IBlockDataProvider* blockProvider)
{
    auto preparedMeta = TSharedMutableRef::Allocate(sizeof(TMeta) * metas.size());
    // Fill with invalid values to crash early when reading uninitialized data.
    memset(preparedMeta.Begin(), 0xfe, sizeof(TMeta) * metas.size());
    auto* preparedMetas = reinterpret_cast<TMeta*>(preparedMeta.begin());

    std::vector<ui32> blockIds;
    std::vector<ui32> segmentPivots;

    // Prepare metas and group by block indexes.
    int lastBlockIndex = -1;
    for (ui32 index = 0; index < metas.size(); ++index) {
        auto blockIndex = metas[index]->block_index();

        if (blockIndex != lastBlockIndex) {
            blockIds.push_back(blockIndex);
            segmentPivots.push_back(index);
            lastBlockIndex = blockIndex;
        }

        const ui64* ptr = nullptr;
        if (blockProvider) {
            ptr = reinterpret_cast<const ui64*>(blockProvider->GetBlock(metas[index]->block_index()) + metas[index]->offset());
        }

        preparedMetas[index].InitFromProto(*metas[index], ptr);
    }

    segmentPivots.push_back(metas.size());

    return {blockIds, segmentPivots, preparedMeta};
}

struct TColumnInfo
{
    std::vector<ui32> SegmentPivots;
    TSharedRef Meta;

    template <EValueType Type>
    TRange<TKeyMeta<Type>> GetKeyMetas()
    {
        return reinterpret_cast<const TKeyMeta<Type>*>(Meta.begin());
    }

    template <EValueType Type>
    TRange<TValueMeta<Type>> GetValueMetas()
    {
        return reinterpret_cast<const TValueMeta<Type>*>(Meta.begin());
    }

    template <EValueType Type>
    struct TPrepareMeta
    {
        static TPrepareResult Do(TSegmentMetas metas, bool valueColumn, bool aggregate, IBlockDataProvider* blockProvider)
        {
            if (valueColumn) {
                if (aggregate) {
                    return DoPrepare<TAggregateValueMeta<Type>>(metas, blockProvider);
                } else {
                    return DoPrepare<TValueMeta<Type>>(metas, blockProvider);
                }
            } else {
                return DoPrepare<TKeyMeta<Type>>(metas, blockProvider);
            }
        }
    };

    std::vector<ui32> PrepareTimestampMetas(TSegmentMetas metas, IBlockDataProvider* blockProvider)
    {
        auto [blockIds, segmentPivots, preparedMeta] = DoPrepare<TTimestampMeta>(metas, blockProvider);

        SegmentPivots = std::move(segmentPivots);
        Meta = std::move(preparedMeta);
        return blockIds;
    }

    std::vector<ui32> PrepareMetas(TSegmentMetas metas, EValueType type, bool value, bool aggregate, IBlockDataProvider* blockProvider)
    {
        auto [blockIds, segmentPivots, preparedMeta] = DispatchByDataType<TPrepareMeta>(type, metas, value, aggregate, blockProvider);

        SegmentPivots = std::move(segmentPivots);
        Meta = std::move(preparedMeta);
        return blockIds;
    }
};

TIntrusivePtr<TPreparedChunkMeta> TPreparedChunkMeta::FromProtoSegmentMetas(
    const NTableClient::TTableSchemaPtr& chunkSchema,
    const NTableClient::TRefCountedColumnMetaPtr& columnMetas,
    const NTableClient::TRefCountedDataBlockMetaPtr& blockMeta,
    IBlockDataProvider* blockProvider)
{
    const auto& chunkSchemaColumns = chunkSchema->Columns();

    THashMap<int, int> firstBlockIdToGroup;

    std::vector<TColumnInfo> preparedColumns;
    // Plus one timestamp column.
    preparedColumns.resize(chunkSchemaColumns.size() + 1);

    std::vector<TColumnGroup> columnGroups;
    std::vector<TColumnGroupInfo> columnGroupInfos;
    columnGroupInfos.resize(chunkSchemaColumns.size() + 1);

    std::vector<std::vector<ui16>> columnIdsPerGroup;

    auto determineColumnGroup = [&] (std::vector<ui32> blockIds, int columnIndex) {
        YT_VERIFY(!blockIds.empty());

        auto [it, inserted] = firstBlockIdToGroup.emplace(blockIds.front(), columnGroups.size());
        if (inserted) {
            columnGroups.emplace_back();
            columnIdsPerGroup.emplace_back();
        }

        auto groupId = it->second;
        columnGroupInfos[columnIndex].GroupId = groupId;

        auto& blockGroup = columnGroups[groupId];

        // Fill BlockIds if blockGroup has been created. Otherwise check that BlockIds and blockIds are equal.
        if (inserted) {
            blockGroup.BlockIds = std::move(blockIds);
        } else {
            YT_VERIFY(blockIds == blockGroup.BlockIds);
        }

        columnGroupInfos[columnIndex].IndexInGroup = columnIdsPerGroup[groupId].size();
        columnIdsPerGroup[groupId].push_back(columnIndex);
    };

    for (int index = 0; index < std::ssize(chunkSchemaColumns); ++index) {
        auto type = GetPhysicalType(chunkSchemaColumns[index].CastToV1Type());
        bool valueColumn = index >= chunkSchema->GetKeyColumnCount();

        auto blockIds = preparedColumns[index].PrepareMetas(
            MakeRange(columnMetas->columns(index).segments()),
            type,
            valueColumn,
            static_cast<bool>(chunkSchemaColumns[index].Aggregate()),
            blockProvider);

        determineColumnGroup(std::move(blockIds), index);
    }

    {
        int timestampReaderIndex = columnMetas->columns().size() - 1;

        auto blockIds = preparedColumns[timestampReaderIndex].PrepareTimestampMetas(
            MakeRange(columnMetas->columns(timestampReaderIndex).segments()),
            blockProvider);

        determineColumnGroup(std::move(blockIds), timestampReaderIndex);
    }

    for (auto& columnGroup : columnGroups) {
        columnGroup.BlockChunkRowCounts.resize(std::ssize(columnGroup.BlockIds));
        for (int index = 0; index < std::ssize(columnGroup.BlockIds); ++index) {
            columnGroup.BlockChunkRowCounts[index] = blockMeta->data_blocks(columnGroup.BlockIds[index]).chunk_row_count();
        }
    }

    std::vector<TRef> blockSegmentMeta;
    for (int groupId = 0; groupId < std::ssize(columnGroups); ++groupId) {
        auto& blockGroup = columnGroups[groupId];

        for (int index = 0; index < std::ssize(blockGroup.BlockIds); ++index) {
            for (auto columnId : columnIdsPerGroup[groupId]) {
                auto& [segmentPivots, meta] = preparedColumns[columnId];

                YT_VERIFY(!segmentPivots.empty());
                auto segmentCount = segmentPivots.back();
                auto segmentSize = meta.Size() / segmentCount;

                auto offset = segmentPivots[index] * segmentSize;
                auto offsetEnd = segmentPivots[index + 1] * segmentSize;

                blockSegmentMeta.push_back(meta.Slice(offset, offsetEnd));
            }

            auto columnCount = blockSegmentMeta.size();

            size_t size = 0;
            for (const auto& metas : blockSegmentMeta) {
                YT_VERIFY(metas.size() % sizeof(ui64) == 0);
                size += metas.size();
            }

            auto offset = sizeof(ui32) * (columnCount + 1);
            auto mergedMeta = TSharedMutableRef::Allocate(offset + size);

            ui32* offsets = reinterpret_cast<ui32*>(mergedMeta.Begin());
            auto* metasData = reinterpret_cast<char*>(mergedMeta.Begin() + offset);

            for (const auto& metas : blockSegmentMeta) {
                *offsets++ = offset;
                std::copy(metas.begin(), metas.end(), metasData);
                offset += metas.size();
                metasData += metas.size();
            }
            *offsets++ = offset;
            blockGroup.MergedMetas.push_back(mergedMeta);

            blockSegmentMeta.clear();
        }

        YT_VERIFY(blockGroup.MergedMetas.size() == blockGroup.BlockIds.size());
    }

    size_t size = columnGroups.capacity() * sizeof(TColumnGroup);
    for (const auto& blockGroup : columnGroups) {
        size += blockGroup.BlockIds.capacity() * sizeof(ui32);
        size += blockGroup.BlockChunkRowCounts.capacity() * sizeof(ui32);
        size += blockGroup.MergedMetas.capacity() * sizeof(TSharedRef);

        for (const auto& perBlockMeta : blockGroup.MergedMetas) {
            size += perBlockMeta.Size();
        }
    }

    return New<TPreparedChunkMeta>(std::move(columnGroups), std::move(columnGroupInfos), size, static_cast<bool>(blockProvider));
}

TIntrusivePtr<TPreparedChunkMeta> TPreparedChunkMeta::FromSegmentMetasStoredInBlocks(
    const NTableClient::TRefCountedColumnGroupInfosExtPtr& columnGroupInfosProto,
    const NTableClient::TRefCountedDataBlockMetaPtr& blockMeta)
{
    std::vector<TColumnGroupInfo> columnGroupInfos(columnGroupInfosProto->column_to_group_size());

    int maxGroupId = 0;
    for (int columnId = 0; columnId < columnGroupInfosProto->column_to_group_size(); ++columnId) {
        auto groupId = columnGroupInfosProto->column_to_group(columnId);
        maxGroupId = std::max(maxGroupId, groupId);
    }

    std::vector<int> columnsInGroup(maxGroupId + 1);
    for (int columnId = 0; columnId < columnGroupInfosProto->column_to_group_size(); ++columnId) {
        auto groupId = columnGroupInfosProto->column_to_group(columnId);
        columnGroupInfos[columnId] = {static_cast<ui16>(groupId), static_cast<ui16>(columnsInGroup[groupId]++)};
    }

    std::vector<TColumnGroup> columnGroups(maxGroupId + 1);
    for (int blockId = 0; blockId < columnGroupInfosProto->block_group_indexes_size(); ++blockId) {
        auto groupId = columnGroupInfosProto->block_group_indexes(blockId);

        auto& columnGroup = columnGroups[groupId];

        columnGroup.BlockIds.push_back(blockId);
        columnGroup.SegmentMetaOffsets.push_back(columnGroupInfosProto->segment_meta_offsets(blockId));
        columnGroup.BlockChunkRowCounts.push_back(blockMeta->data_blocks(blockId).chunk_row_count());
    }

    size_t size = columnGroups.capacity() * sizeof(TColumnGroup);
    for (const auto& blockGroup : columnGroups) {
        size += blockGroup.BlockIds.capacity() * sizeof(ui32);
        size += blockGroup.BlockChunkRowCounts.capacity() * sizeof(ui32);
        size += blockGroup.SegmentMetaOffsets.capacity() * sizeof(ui16);
    }

    return New<TPreparedChunkMeta>(std::move(columnGroups), std::move(columnGroupInfos), size, true);
}

void TPreparedChunkMeta::VerifyEquality(
    const TPreparedChunkMeta& fromProtoMeta,
    const TPreparedChunkMeta& inBlocksMeta,
    const NTableClient::TRefCountedDataBlockMetaPtr& blockMeta)
{
    YT_VERIFY(fromProtoMeta.ColumnGroupInfos.size() == inBlocksMeta.ColumnGroupInfos.size());
    for (int index = 0; index < std::ssize(fromProtoMeta.ColumnGroupInfos); ++index) {
        YT_VERIFY(fromProtoMeta.ColumnGroupInfos[index].GroupId == inBlocksMeta.ColumnGroupInfos[index].GroupId);
        YT_VERIFY(fromProtoMeta.ColumnGroupInfos[index].IndexInGroup == inBlocksMeta.ColumnGroupInfos[index].IndexInGroup);
    }

    YT_VERIFY(fromProtoMeta.ColumnGroups.size() == inBlocksMeta.ColumnGroups.size());
    for (int index = 0; index < std::ssize(fromProtoMeta.ColumnGroups); ++index) {
        YT_VERIFY(fromProtoMeta.ColumnGroups[index].BlockIds.size() == fromProtoMeta.ColumnGroups[index].BlockChunkRowCounts.size());
        YT_VERIFY(fromProtoMeta.ColumnGroups[index].BlockIds.size() == fromProtoMeta.ColumnGroups[index].MergedMetas.size());

        YT_VERIFY(fromProtoMeta.ColumnGroups[index].BlockIds == inBlocksMeta.ColumnGroups[index].BlockIds);
        YT_VERIFY(fromProtoMeta.ColumnGroups[index].BlockChunkRowCounts == inBlocksMeta.ColumnGroups[index].BlockChunkRowCounts);
        YT_VERIFY(fromProtoMeta.ColumnGroups[index].MergedMetas.size() == inBlocksMeta.ColumnGroups[index].SegmentMetaOffsets.size());
    }

    for (int groupId = 0; groupId < std::ssize(inBlocksMeta.ColumnGroups); ++groupId) {
        for (int blockIdIndex = 0; blockIdIndex < std::ssize(inBlocksMeta.ColumnGroups[groupId].BlockIds); ++blockIdIndex) {
            auto blockSize = blockMeta->data_blocks(inBlocksMeta.ColumnGroups[groupId].BlockIds[blockIdIndex]).uncompressed_size();
            auto fromBlockMetaSize = blockSize - inBlocksMeta.ColumnGroups[groupId].SegmentMetaOffsets[blockIdIndex];
            auto fromProtoMetaSize = std::ssize(fromProtoMeta.ColumnGroups[groupId].MergedMetas[blockIdIndex]);
            YT_VERIFY(fromBlockMetaSize == fromProtoMetaSize);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
