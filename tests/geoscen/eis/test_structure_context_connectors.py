from spine.jobs.geoscen.eis.structure_context.connectors.http_client import (
    HttpResponse,
)


def test_http_response_contract() -> None:
    response = HttpResponse(
        url="https://example.invalid",
        status=200,
        payload={"ok": True},
    )

    assert response.status == 200
    assert response.payload == {"ok": True}
