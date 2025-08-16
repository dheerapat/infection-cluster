# cluster-vis

simple infection cluster visualizer

### How to run the project

this project have 2 part
- FastAPI server `server.py`
- Angular frontend `cluster-vis/`

#### Running FastAPI backend

```bash
git clone <this repo>
cd infection-cluster

uv sync

uv run fastapi dev server.py
```

server will run on port 8000

#### Running Angular frontend

open another terminal tab

```bash
# at infection-cluster folder
cd cluster-vis

npm install

ng serve
```

you can access frontend on port 4200