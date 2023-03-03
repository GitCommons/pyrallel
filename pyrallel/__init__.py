import os
import math

import zipfile
from io import BytesIO

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Define the 'drive' variable outside of the function
drive = None

def initialize_auth():
    global drive
    gauth = GoogleAuth()
    # Perform Google authentication and get Drive API client
    gauth.Auth() # gauth.LocalWebserverAuth()  # Assumes authentication through browser #need to handle colab
    drive = GoogleDrive(gauth)

    # Return the authenticated gauth object
    #return gauth


def isMounted(shouldMount=False):
  if drive is None:
    # Call the 'initialize_auth' function to authenticate the user and get Drive API client
    if shouldMount: return initialize_auth()  
    raise Exception("Google Drive API client not initialized. Please call 'initialize_auth' first.")


def get_partition_num(num_partitions):
  """
    Prompt the user to enter a partition number and return a valid integer.

    Args:
      num_partitions (int): The total number of partitions.

    Returns:
      int: The partition number entered by the user.
  """
  try:
    partition_num = int(input(f'Enter a valid partition number (1-{num_partitions}): '))
  except ValueError:
    print('Please enter a valid integer.')
    partition_num = get_partition_num(num_partitions)

  if partition_num < 1 or partition_num > num_partitions:
    partition_num = get_partition_num(num_partitions)

  return partition_num


def uploadHelper(path, data, zip_csv=False):
  """
    Save to file and upload to Google Drive.

    Args:
      path (str): The path of the file on Google Drive.
      data (list): The data to save and upload.
      zip_csv (bool, optional): Whether to zip the CSV file before uploading. Defaults to False.
  """
  isMounted()

  # Create parent folder if it doesn't exist
  parent_folder = os.path.dirname(path)
  if parent_folder != '':
    create_folder(drive, parent_folder)

  # Convert data to CSV string
  csv_str = ''
  for row in data:
    csv_str += ','.join(map(str, row)) + '\n'

  # Zip CSV if requested
  if zip_csv:
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
      zip_file.writestr(os.path.basename(path), csv_str)
    csv_str = zip_buffer.getvalue()

  # Upload CSV to Google Drive
  file = drive.CreateFile({'title': os.path.basename(path), 'parents': [{'id': get_folder_id(drive, parent_folder)}]})
  file.Upload({'content': csv_str})
  print('Uploaded file with ID: {}'.format(file['id']))



def completionChecker(path, partitions):
  # are all files in path

def importHelper(path, partitions):
  """
    Import all partitions into a single array.

    Args:
      path (str): The path of the file on Google Drive.
      partitions (int): The number of partitions.

    Returns:
      list: A list containing all elements from all partitions.
  """
  isMounted()

  file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

  # Find the file with the specified path
  file_id = None
  for file in file_list:
    if file['title'] == path:
      file_id = file['id']
      break

  if not file_id:
    print('Could not find file at path: ' + path)
    return None

  # Initialize empty list to hold partitions
  partitions_list = [None] * partitions

  # Get list of files in the directory
  file_list = drive.ListFile({'q': "'{}' in parents and trashed=false".format(file_id)}).GetList()

  # Import each partition into partitions_list
  for file in file_list:
    # Get partition number from file name
    file_name = file['title']
    partition_num = int(file_name.split('.')[0].split('_')[-1]) - 1

    # Download file contents
    file_id = file['id']
    file_content = drive.CreateFile({'id': file_id}).GetContentString()

    # Convert file contents to list
    partition_list = file_content.split('\n')
    partition_list.remove('')  # remove empty element at end of list

    # Add partition to partitions_list
    partitions_list[partition_num] = partition_list

  # Flatten partitions_list and return result
  return [element for partition in partitions_list for element in partition]

'''
def compilationHelper(path, partitions):
  # Import all partitions
  data = []
  for i in range(1, partitions + 1):
    partition_file_name = f"{path}/partition_{i}.csv"
    with drive.CreateFile({'id': partition_file_id}) as f:
      f.GetContentFile(partition_file_name)
    with open(partition_file_name) as f:
      reader = csv.reader(f)
      partition_data = list(reader)
    data += partition_data

  # Compile and upload the final array
  compiled_file_name = f"{path}/compiled.csv"
  with open(compiled_file_name, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(data)

  uploadHelper(compiled_file_name, 'compiled.zip')
'''

def compile_and_upload(path):
  # Import all partitions
  data = importHelper(path)

  # Compile and upload the final array
  compiled_file_name = f"{path}/compiled.csv"
  with open(compiled_file_name, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(data)

  uploadHelper(compiled_file_name, 'compiled.zip')


def delHelper(path):
  """
    Delete all partitions and leave whole file.

    Args:
      path (str): The path of the file on Google Drive.

    Returns:
      None
  """
  isMounted()

  # Authenticate and get PyDrive client
  drive = authenticate()

  # Get list of files in path
  file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

  # Find the file with the specified path
  file_id = None
  for file in file_list:
    if file['title'] == path:
      file_id = file['id']
      break

  if not file_id:
    print('Could not find file at path: ' + path)
    return

  # Get list of files in the directory
  file_list = drive.ListFile({'q': "'{}' in parents and trashed=false".format(file_id)}).GetList()

  # Delete each file in the directory
  for file in file_list:
    file_id = file['id']
    drive.CreateFile({'id': file_id}).Delete()

  print('All partitions deleted')


def pyrallel(path, array, function, partitions, postCompile=False, finDel=False):
  """
    Run a function on an array in parallel partitions and upload the results to Google Drive.

    Args:
      path (str): The path of the file on Google Drive to upload the results to.
      array (list): The array to run the function on.
      function (function): The function to run on the array.
      partitions (int): The number of partitions to split the array into.
      postCompile (bool, optional): Whether to call compilationHelper() after running the function on the array. Defaults to False.
      finDel (bool, optional): Whether to call delHelper() after uploading the results to Google Drive. Defaults to False.
      zip_csv (bool, optional): Whether to zip the CSV file before uploading. Defaults to False.
  """
  global drive
  isMounted(True)
    
  # Get partition size and number of partitions
  partition_size = math.ceil(len(array) / partitions)
  num_partitions = math.ceil(len(array) / partition_size)

  # Get partition slice
  partition_num = get_partition_num(num_partitions)
  start_index = (partition_num - 1) * partition_size
  end_index = min(partition_num * partition_size, len(array))
  partition = array[start_index:end_index]

  # Run function on partition
  output_partition = function(partition)

  # Upload partition results to Google Drive
  partition_path = f'{os.path.splitext(path)[0]}_{partition_num}.csv'  # hmm project_num_of_total.csv
  uploadHelper(partition_path, output_partition, zip_csv=zip_csv)

   # Call compilationHelper() if requested
  

  if completionChecker(os.path.dirname(partition_path), num_partitions):
    # If all partitions are completed and...
    if postCompile:
      # postCompile is True, compile all partitions into a single file
      compilationHelper()

    if finDel:
       # finDel is True, delete all partitions and leave the whole file
      delHelper()
  else:
    print('Partition ' + str(partition_num) + ' completed')


def pyrallel(path, array, function, partitions, postCompile=False, finDel=False, zip_csv=False):
  