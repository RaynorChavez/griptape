from typing import Optional
from griptape import utils
from griptape.drivers import BaseVectorStoreDriver
from griptape.artifacts import TextArtifact
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
        doc = {
            "_id": vector_id, 
            "Description": string,  # Description will be treated as tensor field
        }

        # Non-tensor fields
        if meta:
            doc["meta"] = str(meta)
        if namespace:
            doc['namespace'] = namespace

        return self.mq.index(self.index).add_documents([doc], non_tensor_fields=["meta", "namespace"])

    def upsert_text_artifact(
            self,
            artifact: TextArtifact,
            namespace: Optional[str] = None,
            meta: Optional[dict] = None,
            **kwargs
    ) -> str:

        artifact_json = artifact.to_json()


        doc = {
            "_id": artifact.id, 
            "Description": artifact.value,  # Description will be treated as tensor field
            "artifact": str(artifact_json),
            "namespace": namespace
        }

        return self.mq.index(self.index).add_documents([doc], non_tensor_fields=["meta", "namespace","artifact"])


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

        filter_string = f"namespace:{namespace}" if namespace else None
        results = self.mq.index(self.index).search("", limit=10000, filter_string=filter_string)

        # get all _id's from search results
        ids = [r["_id"] for r in results["hits"]]

        # get documents corresponding to the ids
        documents = self.mq.index(self.index).get_documents(document_ids=ids,expose_facets=True)

        # for each document, if it's found, create an Entry object
        entries = []
        for doc in documents['results']:
            if doc['_found']:
                entries.append(
                    BaseVectorStoreDriver.Entry(
                        id=doc["_id"],
                        vector=doc["_tensor_facets"][0]["_embedding"],
                        meta={k: v for k, v in doc.items() if k not in ["_id", "_tensor_facets", "_found"]},
                        namespace=doc.get("namespace"),
                    )
                )

        return entries

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
            "attributes_to_retrieve": ["*"] if include_metadata else ["_id"],
            "filter_string": f"namespace:{namespace}" if namespace else None
        } | kwargs

        results = self.mq.index(self.index).search(query, **params)

        if include_vectors:
            results = self.mq.index(self.index).get_documents(list(map(lambda x: x["_id"], results)))

        return [
            BaseVectorStoreDriver.QueryResult(
                vector=None,  # update this line depending on how you access the vector
                score=r["_score"],
                meta={k: v for k, v in r.items() if k not in ["_score"]},
            )
            for r in results["hits"]
        ]

    def create_index(self, name: str, **kwargs) -> None:
        result = self.mq.create_index(name, settings_dict=kwargs)
        return result

    def delete_index(self, name: str) -> None:
        result = self.mq.delete_index(name)
        return result
    
    def get_indexes(self):
        # Still buggy, does not return proper dict. 
        # Will not be able to check whether an index already exists in the marqo instance or not
        # When this is implemented, also change set_index to automatically create a new index if 
        # it does not yet exist, or just set self.index = index if it does.
        indexes = [list(index) for index in self.mq.get_indexes()]
        return indexes

    def upsert_vector(
            self,
            vector: list[float],
            vector_id: Optional[str] = None,
            namespace: Optional[str] = None,
            meta: Optional[dict] = None,
            **kwargs
    ) -> str:
        raise Exception("not implemented")