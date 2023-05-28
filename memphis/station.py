import json

from memphis.exceptions import MemphisError
from memphis.utils import get_internal_name


class Station:
    def __init__(self, connection, name: str):
        self.connection = connection
        self.name = name.lower()

    async def destroy(self):
        """Destroy the station."""
        try:
            name_req = {"station_name": self.name, "username": self.connection.username}
            station_name = json.dumps(name_req, indent=2).encode("utf-8")
            res = await self.connection.broker_manager.request(
                "$memphis_station_destructions", station_name, timeout=5
            )
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise MemphisError(error)

            internal_station_name = get_internal_name(self.name)
            sub = self.connection.schema_updates_subs.get(internal_station_name)
            task = self.connection.schema_tasks.get(internal_station_name)
            if internal_station_name in self.connection.schema_updates_data:
                del self.connection.schema_updates_data[internal_station_name]
            if internal_station_name in self.connection.schema_updates_subs:
                del self.connection.schema_updates_subs[internal_station_name]
            if internal_station_name in self.connection.producers_per_station:
                del self.connection.producers_per_station[internal_station_name]
            if internal_station_name in self.connection.schema_tasks:
                del self.connection.schema_tasks[internal_station_name]
            if task is not None:
                task.cancel()
            if sub is not None:
                await sub.unsubscribe()

            self.connection.producers_map = {
                k: v
                for k, v in self.connection.producers_map.items()
                if self.name not in k
            }

            self.connection.consumers_map = {
                k: v
                for k, v in self.connection.consumers_map.items()
                if self.name not in k
            }

        except Exception as e:
            raise MemphisError(str(e)) from e
