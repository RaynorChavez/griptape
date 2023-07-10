import pytest
from griptape.artifacts import TextArtifact
from griptape.drivers import MarqoVectorStoreDriver
from marqo import Client

class TestMarqoVectorStorageDriver:

    @pytest.fixture(autouse=True)
    def marqo_client(self):
        client = Client(api_key="foobar", url="http://localhost:8882")
        return client

    @pytest.fixture
    def driver(self, marqo_client):
        return MarqoVectorStoreDriver(
            api_key="foobar",
            url="http://localhost:8882",
            index="test",
            mq=marqo_client
        )

    
    def test_create_index(self, driver):
        try:
            result = driver.create_index("my-first-index")
            assert result["acknowledged"] == True
            assert result["index"] == "my-first-index"
        except:
            pass

    @pytest.fixture
    def insert_data(self, driver):
        # Add a document to the Marqo database
        document_id = "my-test-doc"
        text = "Test text"
        driver.upsert_text(text, vector_id=document_id)
        return document_id

    def test_upsert_text(self, driver):
        driver.set_index("my-first-index")
        vector_id = "doc1"
        result = driver.upsert_text("test text document 1 cats are nice", vector_id=vector_id)
        assert result['items'][0]['_id'] == vector_id

        vector_id = "doc2"
        result = driver.upsert_text("test text document 1 dogs are nice", vector_id=vector_id)
        assert result['items'][0]['_id'] == vector_id

    def test_search(self, driver):
        driver.set_index("my-first-index")
        query = "cats"
        results = driver.query(query)
        print("test search ", query, ": \n", results)
        assert len(results) > 0
        assert results[0].score > 0
        assert "_id" in results[0].meta
        assert "text" in results[0].meta

    def test_get_document(self, driver):
        driver.set_index("my-first-index")

        vector_id = "doc3"
        result = driver.upsert_text("test text document 1 turtles are nice", vector_id=vector_id)
        assert result['items'][0]['_id'] == vector_id

        document_id = "doc3"
        entry = driver.load_entry(document_id)
        print("test get document: \n", entry)
        assert entry.id == document_id
        assert "text" in entry.meta
        assert isinstance(entry.vector, list) and len(entry.vector) > 0
