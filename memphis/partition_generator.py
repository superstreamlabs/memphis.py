

class PartitionGenerator:
    def __init__(self, partitions_list):
        self.partitions_list = partitions_list
        self.current = 0
        self.length = len(partitions_list)

    def next(self):
        partition_to_return = self.partitions_list[self.current]
        self.current = (self.current + 1) % self.length
        return partition_to_return