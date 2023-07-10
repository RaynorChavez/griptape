from typing import Optional
from griptape import utils
from griptape.drivers import BaseVectorStoreDriver
import marqo
from attr import define, field


@define
class MarqoVectorStoreDriver(BaseVectorStoreDriver):
    api_key: str = field(kw_only=True)
    url: str = field(kw_only=True)
    mq: marqo.Client = field(kw_only=True)
    index: str = field(kw_only=True)

    def __attrs_post_init__(self):
        self.mq = marqo.Client(self.url, self.api_key)

    def set_index(self, index):
        self.index = index

    def upsert_text(
            self,
            string: str,
            vector_id: Optional[str] = None,
            namespace: Optional[str] = None,
            meta: Optional[dict] = None,
            **kwargs
    ) -> str:
        doc = {"_id": vector_id, "text": string} # implement the Title: ,Description: format?
        if namespace is not None:
            doc['namespace'] = namespace
        return self.mq.index(self.index).add_documents([doc])


    def load_entry(self, vector_id: str, namespace: Optional[str] = None) -> Optional[BaseVectorStoreDriver.Entry]:
        result = self.mq.index(self.index).get_document(document_id=vector_id, expose_facets=True)

        if result and "_tensor_facets" in result and len(result["_tensor_facets"]) > 0:
            return BaseVectorStoreDriver.Entry(
                id=result.get("_id"),
                meta={k: v for k, v in result.items() if k not in ["_id"]},
                vector=result["_tensor_facets"][0]["_embedding"],
            )
        else:
            return None


    def load_entries(self, namespace: Optional[str] = None) -> list[BaseVectorStoreDriver.Entry]:

        results = self.mq.index(self.index).get_documents(
            "",
            limit=10000
        )

        return [
            BaseVectorStoreDriver.Entry(
                id=r["_id"],
                vector=r["values"],
                meta=r["metadata"],
                namespace=results["namespace"]
            )
            for r in results["matches"]
        ]

    def query(
            self,
            query: str,
            count: Optional[int] = None,
            namespace: Optional[str] = None,
            include_vectors: bool = False,
            include_metadata=True,
            **kwargs
    ) -> list[BaseVectorStoreDriver.QueryResult]:

        params = {
            "limit": count if count else BaseVectorStoreDriver.DEFAULT_QUERY_COUNT,
            "attributes_to_retrieve": ["*"] if include_metadata else ["_id"]
        } | kwargs

        results = self.mq.index(self.index).search(query, **params)

        if include_vectors:
            results = self.mq.index(self.index).get_documents(list(map(lambda x: x["_id"], results)))

        return [
            BaseVectorStoreDriver.QueryResult(
                vector=None,  # update this line depending on how you access the vector
                score=r["_score"],
                meta={k: v for k, v in r.items() if k not in ["_score"]},
                #id=r["_id"], how should this work?
            )
            for r in results["hits"]
        ]

    def create_index(self, name: str, **kwargs) -> None:
        result = self.mq.create_index(name, settings_dict=kwargs)
        #print(result)
        return result

    def upsert_vector(
            self,
            vector: list[float],
            vector_id: Optional[str] = None,
            namespace: Optional[str] = None,
            meta: Optional[dict] = None,
            **kwargs
    ) -> str:
        raise Exception("not implemented")