import numpy as np

# Create a multidimensional array
arr = np.array([[1, 7, 5], [2, 4, 6]])

# Find the maximum element along a specified axis
result = np.maximum.reduce(arr, axis=0)

print(result)