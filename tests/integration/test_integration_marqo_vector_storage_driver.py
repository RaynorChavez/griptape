import pytest
from griptape.artifacts import TextArtifact
from griptape.drivers import MarqoVectorStoreDriver
from marqo import Client

class TestMarqoVectorStorageDriver:
    
    @pytest.fixture(autouse=True)
    def marqo_client(self):
        client = Client(url="http://localhost:8882")

        # client.create_index("marqo-griptape-test-index")

        yield client
        
        # try:
        #     client.delete_index("marqo-griptape-test-index")
        # except Exception:
        #     pass

    @pytest.fixture
    def driver(self, marqo_client) -> MarqoVectorStoreDriver:
        return MarqoVectorStoreDriver(
            url="http://localhost:8882",
            index="marqo-griptape-test-index",
            mq=marqo_client
        )

    @pytest.mark.integrationtest
    def test_create_index(self, driver: MarqoVectorStoreDriver):
        try:
            driver.mq.delete_index("marqo-griptape-test-index")
        except Exception:
            pass

        result = driver.create_index("marqo-griptape-test-index")
        assert result["acknowledged"] == True
        assert result["index"] == "marqo-griptape-test-index"

    @pytest.fixture
    def insert_data(self, driver: MarqoVectorStoreDriver):
        # Add a document to the Marqo database
        document_id = "my-test-doc"
        text = "Test text"
        driver.upsert_text(text, vector_id=document_id)
        return document_id

    @pytest.mark.integrationtest
    def test_upsert_text(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")
        vector_id = "doc1"
        result = driver.upsert_text("test text document 1 cats are nice", vector_id=vector_id)
        assert result['items'][0]['_id'] == vector_id

        vector_id = "doc2"
        result = driver.upsert_text("test text document 1 dogs are nice", vector_id=vector_id)
        assert result['items'][0]['_id'] == vector_id

    @pytest.mark.integrationtest
    def test_upsert_text_artifact(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")
        namespace = "marqo-namespace"
        # Arrange
        text = TextArtifact(id="a44b04ff052e4109b3c6fda0f3f3e997", value="racoons")
        
        # Act
        result = driver.upsert_text_artifact(text, namespace=namespace)
        
        # Assert: Check that the document was added successfully
        assert result['items'][0]['_id'] == text.id
        assert result['items'][0]['result'] == "created" or "updated"

    @pytest.mark.integrationtest  
    def test_upsert_text_artifacts(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")
        namespace = "marqo-namespace"

        # Arrange
        texts = [
            TextArtifact(id="doc5", value="Cats are great."),
            TextArtifact(id="doc6", value="Dogs are also great."),
            TextArtifact(id="doc7", value="Turtles are awesome.")
        ]
        
        # Act and Assert: Check that each document is added successfully
        for text in texts:
            result = driver.upsert_text_artifact(text, namespace=namespace)
            assert result['items'][0]['_id'] == text.id
            assert result['items'][0]['result'] == "created" or "updated"

    @pytest.mark.integrationtest
    def test_search(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")
        query = "cats"
        results = driver.query(query)
        print("test search ", query, ": \n", results)
        assert len(results) > 0
        assert results[0].score > 0
        assert "_id" in results[0].meta
        assert "Description" in results[0].meta

    @pytest.mark.integrationtest
    def test_load_entry(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")

        vector_id = "doc3"
        result = driver.upsert_text("test text document 1 turtles are nice", vector_id=vector_id)
        assert result['items'][0]['_id'] == vector_id

        document_id = "doc3"
        entry = driver.load_entry(document_id)
        print("test get document: \n", entry)
        assert entry.id == document_id
        assert "Description" in entry.meta
        assert isinstance(entry.vector, list) and len(entry.vector) > 0

    @pytest.mark.integrationtest
    def test_load_entries(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")
        # Act
        entries = driver.load_entries()
        print(entries)

        # Assert
        assert len(entries) > 0  # Check that some entries are returned
        for entry in entries:
            assert entry.id  # Each entry should have an id
            assert entry.vector  # Each entry should have a vector
            assert entry.meta  # Each entry should have meta data
            assert "Description" in entry.meta  # Change 'text' to 'Description'

    @pytest.mark.integrationtest
    def test_namespace_filtering_query(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")

        # Arrange: Insert documents with different namespaces
        vector_id_1 = "doc1"
        vector_id_2 = "doc2"
        driver.upsert_text("test text document 1 cats are nice", vector_id=vector_id_1, namespace="ns1")
        driver.upsert_text("test text document 2 dogs are nice", vector_id=vector_id_2, namespace="ns2")

        # Act: Query with namespace filtering
        results_ns1 = driver.query("nice", namespace="ns1")
        results_ns2 = driver.query("nice", namespace="ns2")

        # Assert: The results should only include documents from the filtered namespace
        assert len(results_ns1) == 1
        assert len(results_ns2) == 1
        assert all(res.meta["namespace"] == "ns1" for res in results_ns1)
        assert all(res.meta["namespace"] == "ns2" for res in results_ns2)

    @pytest.mark.integrationtest
    def test_namespace_filtering_load_entries(self, driver: MarqoVectorStoreDriver):
        driver.set_index("marqo-griptape-test-index")

        # Arrange: Insert documents with different namespaces
        vector_id_1 = "doc1"
        vector_id_2 = "doc2"
        driver.upsert_text("test text document 1 cats are nice", vector_id=vector_id_1, namespace="ns1")
        driver.upsert_text("test text document 2 dogs are nice", vector_id=vector_id_2, namespace="ns2")

        # Act: Load entries with namespace filtering
        entries_ns1 = driver.load_entries(namespace="ns1")
        entries_ns2 = driver.load_entries(namespace="ns2")

        # Assert: The loaded entries should only include documents from the filtered namespace
        assert len(entries_ns1) == 1
        assert len(entries_ns2) == 1
        assert all(entry.namespace == "ns1" for entry in entries_ns1)
        assert all(entry.namespace == "ns2" for entry in entries_ns2)
