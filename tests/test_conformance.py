import pytest
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.capabilities import Capability
from langgraph_checkpoint_opengauss import AsyncOpenGaussSaver, OpenGaussSaver


class SyncSuite(OpenGaussSaver):
    async def aput(self, *args, **kwargs):
        return self.put(*args, **kwargs)

    async def aput_writes(self, *args, **kwargs):
        return self.put_writes(*args, **kwargs)

    async def aget_tuple(self, *args, **kwargs):
        return self.get_tuple(*args, **kwargs)

    async def alist(self, *args, **kwargs):
        for checkpoint in self.list(*args, **kwargs):
            yield checkpoint

    async def adelete_thread(self, *args, **kwargs):
        return self.delete_thread(*args, **kwargs)

    async def acopy_thread(self, *args, **kwargs):
        return self.copy_thread(*args, **kwargs)

    async def adelete_for_runs(self, *args, **kwargs):
        return self.delete_for_runs(*args, **kwargs)

    async def aprune(self, *args, **kwargs):
        return self.prune(*args, **kwargs)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_conformance(mode, schema_factory, run_async):
    @checkpointer_test(name=mode)
    async def factory():
        with schema_factory() as dsn:
            if mode == "sync":
                with SyncSuite.from_conn_string(dsn) as saver:
                    saver.setup()
                    yield saver
            else:
                async with AsyncOpenGaussSaver.from_conn_string(dsn) as saver:
                    await saver.setup()
                    yield saver

    report = run_async(validate(factory))
    assert set(report.results) == {cap.value for cap in Capability}
    for capability, result in report.results.items():
        print(
            f"{mode} {capability}: {result.tests_passed} passed, {result.tests_failed} failed"
        )
    failures = [
        error for result in report.results.values() for error in result.failures
    ]
    assert not failures, "\n".join(failures)
    assert all(
        result.detected
        and result.passed
        and result.tests_passed > 0
        and result.tests_skipped == 0
        for result in report.results.values()
    )
