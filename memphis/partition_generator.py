#The PartitionGenerator class is used to create a round robin generator for station's partitions
#the class gets a list of partitions and by using the next() function it returns an item from the list

class PartitionGenerator:
    def __init__(self, partitions_list):
        self.partitions_list = partitions_list
        self.current = 0
        self.length = len(partitions_list)

    def __next__(self):
        partition_to_return = self.partitions_list[self.current]
        self.current = (self.current + 1) % self.length
        return partition_to_return
    