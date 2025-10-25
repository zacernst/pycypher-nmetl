"""Just a test."""

from __future__ import annotations

import base64
import uuid
from typing import Any, Generator, Optional

# import pickle
import dill as pickle
import zmq
from nmetl.logger import LOGGER
from pycypher.query import NullResult

LOGGER.setLevel("WARNING")


class Shutdown:
    pass


class ZMQMessage:
    """Wraps the message with a little metadata."""

    def __init__(self, queue_name: str = "", contents: Any = None) -> None:
        self.queue_name = queue_name
        self.contents = contents

    @classmethod
    def encode(cls, obj: Any) -> str:
        """Encode into a string."""
        out: str = base64.b64encode(pickle.dumps(obj)).decode("utf8")
        return out

    @classmethod
    def decode(cls, obj: bytes) -> ZMQMessage:
        out: Any = pickle.loads(base64.b64decode(obj.decode()))
        if not isinstance(out, ZMQMessage):
            raise TypeError(
                f"Expected to decode a ZMQMessage, got: {out.__class__}"
            )
        return out


class QueueGenerator:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Let's try ZMQ"""

    def __init__(
        self,
        *args,  # pylint: disable=unused-argument
        name: str = uuid.uuid4().hex,
        port: Optional[int | str] = None,
        host: str = "localhost",
        **kwargs,  # pylint: disable=unused-argument
    ) -> None:
        if not port:
            raise ValueError("`port` argument is required for QueueGenerator")

        self._port: str = str(port)
        self.host: str = host

        self.put_context = zmq.Context()  # pyrefly: ignore
        self.get_context = zmq.Context()  # pyrefly: ignore

        self.put_socket = self.put_context.socket(zmq.PUSH)  # pyrefly: ignore
        self.put_socket.bind(f"tcp://*:{port}")

        self.get_socket = self.get_context.socket(zmq.PULL)  # pyrefly: ignore
        self.get_socket.connect(f"tcp://{host}:{port}")

        self.name = name

    def get(self, **kwargs) -> Any:
        """Get an item from the queue."""
        message: ZMQMessage = ZMQMessage.decode(self.get_socket.recv())
        LOGGER.debug(f"Received {message.contents}:::{self.name}")
        while message.queue_name != self.name:
            message = ZMQMessage.decode(self.get_socket.recv())
            LOGGER.debug(f"Received {message.contents}")
        item: Any = message.contents
        return item

    def put(self, item: Any) -> None:
        """Put an item on the queue."""
        if item is None or isinstance(item, NullResult):
            return
        message: ZMQMessage = ZMQMessage(queue_name=self.name, contents=item)
        encoded_message: str = ZMQMessage.encode(message)
        LOGGER.info("Putting message on %s", message.queue_name)
        self.put_socket.send_string(encoded_message)

    def yield_items(self) -> Generator[Any, None, None]:
        """Generate items."""
        LOGGER.info("YIELD ITEMS CALLED %s", self.name)
        while 1:
            item: Any = self.get()
            if item is None:
                continue
            if isinstance(item, Shutdown):
                break
            yield item


if __name__ == "__main__":
    q: QueueGenerator = QueueGenerator(name="hithere")
    message: ZMQMessage = ZMQMessage(queue_name="hithere", contents="whatever")
    for _ in range(1000):
        q.put(message)
    q.put(Shutdown())
    counter = 0
    for i in q.yield_items():
        print(counter, i)
        counter += 1
