"""BQ Writer"""

from typing import Iterable, Type
from datetime import datetime
from google.cloud.bigquery_storage import (
    BigQueryWriteClient,
    AppendRowsRequest,
    WriteStream,
    ProtoRows,
    ProtoSchema,
)
from google.protobuf import descriptor_pb2
import model.bqmodel_pb2 as BQ_MODEL
from model.base import BaseTask
from core.logger import logger as log
from utils.helpers import timestamp_to_integer

PROTO_DESCRIPTOR = descriptor_pb2.DescriptorProto()


class Writer(BaseTask):
    def __init__(self, project: str, dataset: str, table: str, model: str) -> None:
        self.project = project
        self.dataset = dataset
        self.table = table
        self.model = self.get_model(model.capitalize())
        self.client = None
        self.stream = None

    def get_model(self, model: str) -> Type:
        """Get BQ write model

        :param str model: Model name in BQ_MODEL
        :raises AttributeError: If model name is not found/ supported
        :return _type_: _description_
        """
        if not hasattr(BQ_MODEL, model):
            raise AttributeError("Unsupported model: {model} in BQ model")

        bqmodel = getattr(BQ_MODEL, model)
        bqmodel.DESCRIPTOR.CopyToProto(PROTO_DESCRIPTOR)
        log.info(f"Using BQ proto model: {bqmodel}")
        return bqmodel

    def get_client(self):
        """Get BQ client

        :return _type_: _description_
        """
        client = BigQueryWriteClient()
        log.info(f"Initiated BigQueryWriteClient: {client}")
        return client

    def get_stream(self, client: BigQueryWriteClient) -> Type:
        """Get write stream

        :param BigQueryWriteClient client: BQ client
        """
        parent = client.table_path(self.project, self.dataset, self.table)
        write_stream = client.create_write_stream(
            parent=parent, write_stream=WriteStream(type_=WriteStream.Type.COMMITTED)
        )
        log.info(
            f"Initiated write_stream: {write_stream.name}, parent: {parent}, type: COMMITTED!"
        )
        return write_stream

    def run(self, messages: Iterable[dict]):
        if not self.client:
            self.client = self.get_client()

        if not self.stream:
            self.stream = self.get_stream(self.client)

        requests, items = [], []
        PROTO_SCHEMA = ProtoSchema(proto_descriptor=PROTO_DESCRIPTOR)
        for message in messages:
            item = self.model()
            for k, v in message.items():
                if isinstance(v, list):
                    getattr(item, k).extend(v)
                    continue

                if isinstance(v, datetime):
                    v = timestamp_to_integer(v)
                setattr(item, k, v)

            items.append(item.SerializeToString())
            log.debug(f"Inserting message: {item}")

        data = AppendRowsRequest.ProtoData(
            writer_schema=PROTO_SCHEMA, rows=ProtoRows(serialized_rows=items)
        )
        requests.append(
            AppendRowsRequest(write_stream=self.stream.name, proto_rows=data)
        )

        total_count, failed_count = len(items), 0
        for resp in self.client.append_rows(requests=iter(requests)):
            if resp.error or resp.row_errors:
                failed_count += 1
                log.error(resp)
            else:
                log.debug(resp)

        log.info(f"Written {total_count} total rows, {failed_count} failed")
