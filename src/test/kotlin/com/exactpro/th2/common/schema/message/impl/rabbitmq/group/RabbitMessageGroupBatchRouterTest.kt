package com.exactpro.th2.common.schema.message.impl.rabbitmq.group

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation
import com.exactpro.th2.common.schema.message.configuration.MqRouterFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

class RabbitMessageGroupBatchRouterTest : RabbitMessageGroupBatchRouter() {

    // based on TS-1009 issue
    @Test
    fun splitAndFilter() {
        val rawMessage = RawMessage.newBuilder().apply {
            metadataBuilder.idBuilder.connectionIdBuilder.setSessionAlias("http_server")
            metadataBuilder.putProperties("method", "POST")
        }

        val anyMessage = AnyMessage.newBuilder().setRawMessage(rawMessage)
        val group = MessageGroup.newBuilder().addMessages(anyMessage)
        val batch = MessageGroupBatch.newBuilder().addGroups(group).build()

        val filterPost = FieldFilterConfiguration("method", "POST", FieldFilterOperation.EQUAL)
        val filterNotPost = FieldFilterConfiguration("method", "POST", FieldFilterOperation.NOT_EQUAL)

        val filterSessionEqual = FieldFilterConfiguration("session_alias", "http_server", FieldFilterOperation.EQUAL)

        val filtersForOutCodecDecode = listOf(
            MqRouterFilterConfiguration(
                message = listOf(filterNotPost),
                metadata = listOf(filterSessionEqual)
            )
        )

        val pinOutCodecDecodeConfig = QueueConfiguration(
            "routing_key_01",
            "queue_01",
            "exchange_01",
            listOf("decoder_out", "publish"),
            filtersForOutCodecDecode
        )

        val filterForOutCodecDecodeTruelayer = listOf(
            MqRouterFilterConfiguration(
                message = listOf(filterPost),
                metadata = listOf(filterSessionEqual)
            )
        )

        val pinOutCodecDecodeTruelayerConfig = QueueConfiguration(
            "routing_key_01",
            "queue_01",
            "exchange_01",
            listOf("decoder_out", "publish"),
            filterForOutCodecDecodeTruelayer
        )

        val filteredBatchOutCodecDecode = splitAndFilter(
            batch,
            pinOutCodecDecodeConfig,
            "out_codec_decode"
        )

        val filteredBatchOutCodecDecodeTruelayer = splitAndFilter(
            batch,
            pinOutCodecDecodeTruelayerConfig,
            "out_codec_decode"
        )

        assertNull(filteredBatchOutCodecDecode)
        assertEquals(1, filteredBatchOutCodecDecodeTruelayer?.groupsCount)
    }
}