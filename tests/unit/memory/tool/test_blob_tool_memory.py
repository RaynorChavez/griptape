from griptape.artifacts import BlobArtifact
from griptape.drivers import LocalBlobToolMemoryDriver
from griptape.memory.tool import BlobToolMemory
from griptape.tasks import ActionSubtask
from tests.mocks.mock_tool.tool import MockTool


class TestBlobToolMemory:
    def test_init(self):
        memory = BlobToolMemory(driver=LocalBlobToolMemoryDriver())

        assert memory.id == BlobToolMemory.__name__

        memory = BlobToolMemory(id="MyMemory", driver=LocalBlobToolMemoryDriver())

        assert memory.id == "MyMemory"

    def test_process_output(self):
        memory = BlobToolMemory(id="MyMemory", driver=LocalBlobToolMemoryDriver())
        artifact = BlobArtifact(b"foo", name="foo")
        subtask = ActionSubtask()
        output = memory.process_output(MockTool().test, subtask, artifact)

        assert output.to_text().startswith(
            'Output of "MockTool.test" was stored in memory "MyMemory" with the following artifact namespace:'
        )
        assert memory.namespace_metadata[artifact.id] == subtask.to_json()

        assert memory.driver.load(artifact.id) == [artifact]

    def test_process_output_with_many_artifacts(self):
        memory = BlobToolMemory(id="MyMemory", driver=LocalBlobToolMemoryDriver())

        assert memory.process_output(
            MockTool().test, ActionSubtask(), [BlobArtifact(b"foo", name="foo")]
        ).to_text().startswith(
            'Output of "MockTool.test" was stored in memory "MyMemory" with the following artifact namespace:'
        )
