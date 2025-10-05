from pymemcache.client import base

# Kết nối 2 node
node1 = base.Client(('localhost', 11211))
node2 = base.Client(('localhost', 11212))

# Ghi dữ liệu vào node 1
node1.set('user', 'Khoa Nguyen')

# Đọc từ node 1
print("Node1:", node1.get('user'))

# Đọc từ node 2 (chưa có)
print("Node2:", node2.get('user'))
