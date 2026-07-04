def test_register_and_login(client):
    resp = client.post("/api/auth/register", json={
        "email": "alice@example.com", "password": "supersecret1", "full_name": "Alice",
    })
    assert resp.status_code == 201
    assert resp.json()["email"] == "alice@example.com"

    resp = client.post("/api/auth/login", data={"username": "alice@example.com", "password": "supersecret1"})
    assert resp.status_code == 200
    assert "access_token" in resp.json()


def test_login_fails_with_wrong_password(client):
    client.post("/api/auth/register", json={"email": "bob@example.com", "password": "correctpass1"})
    resp = client.post("/api/auth/login", data={"username": "bob@example.com", "password": "wrongpass"})
    assert resp.status_code == 401


def test_protected_endpoint_requires_token(client):
    resp = client.get("/api/organizations")
    assert resp.status_code == 401


def test_end_to_end_project_queue_job_flow(client, auth_headers):
    org = client.post("/api/organizations", json={"name": "Acme Inc"}, headers=auth_headers).json()
    project = client.post(
        "/api/projects", json={"name": "Main", "organization_id": org["id"]}, headers=auth_headers
    ).json()
    queue = client.post(
        "/api/queues",
        json={"name": "default", "project_id": project["id"], "priority": 1, "concurrency_limit": 3},
        headers=auth_headers,
    ).json()
    assert queue["is_paused"] is False

    job = client.post(
        "/api/jobs",
        json={"queue_id": queue["id"], "task_name": "sum_numbers", "payload": {"numbers": [1, 2, 3]}},
        headers=auth_headers,
    ).json()
    assert job["status"] == "queued"

    stats = client.get(f"/api/queues/{queue['id']}/stats", headers=auth_headers).json()
    assert stats["queued"] == 1

    pause_resp = client.post(f"/api/queues/{queue['id']}/pause", headers=auth_headers)
    assert pause_resp.json()["is_paused"] is True


def test_worker_registration_and_claim(client, auth_headers):
    org = client.post("/api/organizations", json={"name": "Acme"}, headers=auth_headers).json()
    project = client.post(
        "/api/projects", json={"name": "P", "organization_id": org["id"]}, headers=auth_headers
    ).json()
    queue = client.post(
        "/api/queues", json={"name": "q1", "project_id": project["id"]}, headers=auth_headers
    ).json()
    client.post(
        "/api/jobs",
        json={"queue_id": queue["id"], "task_name": "sum_numbers", "payload": {"numbers": [1, 2]}},
        headers=auth_headers,
    )

    worker = client.post("/api/workers/register", json={"name": "w1", "concurrency": 2}).json()
    claimed = client.post(f"/api/workers/{worker['id']}/claim").json()
    assert claimed is not None
    assert claimed["status"] == "claimed"

    # A second claim attempt should find nothing left.
    claimed_again = client.post(f"/api/workers/{worker['id']}/claim").json()
    assert claimed_again is None
