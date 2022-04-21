import os
import re

from flask import Flask, request, Response
from werkzeug.exceptions import BadRequest
from typing import Iterator, Union

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

payload = {
  'file_name': 'apache_logs.txt',
  'cmd1': 'filter',
  'value1': 'GET',
  'cmd2': 'map',
  'value2': '0'
}


def build_query(it: Iterator, cmd: str, value: str) -> Iterator:
    if cmd == "filter":
        return filter(lambda v: value in v, it)
    if cmd == "map":
        return map(lambda v: v.split(" ")[int(value)], it)
    if cmd == "unique":
        return iter(set(it))
    if cmd == "sort":
        reverse = value == "desc"
        return iter(sorted(it, reverse=reverse))
    if cmd == "limit":
        return get_limit(it, int(value))
    if cmd == "regex":
        regex = re.compile(value)
        return filter(lambda v: regex.findall(v), it)
    return it


def get_limit(it: Iterator, value: int) -> Iterator:
    i = 0
    for item in it:
        if i < value:
            yield item
        else:
            break
        i += 1


@app.post("/perform_query")
def perform_query() -> Union[BadRequest, Response]:
    try:
        cmd1 = request.args["cmd1"]
        cmd2 = request.args["cmd2"]
        value1 = request.args["value1"]
        value2 = request.args["value2"]
        file_name = request.args["file_name"]
    except KeyError:
        raise BadRequest

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        return BadRequest(description=f"{file_name} was not found")

    with open(file_path) as fd:
        res = build_query(fd, cmd1, value1)
        res = build_query(res, cmd2, value2)
        content = '\n'.join(res)
        print(content)
    return app.response_class(content, content_type="text/plain")
