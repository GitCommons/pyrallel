# pyrallel
Easily run dataset subsections and upload them all to a shared remote.

```sh
pip install git+https://github.com/GitCommons/pyrallel.git@master
```


**args: name of project, array, function, partitions, *{compile when done, delete snapshots when compiled}***
0. ask which partition number to run (int)
1. slice arr (validate arr)
2. run function on arr (expect arr return?)
3. save to file? (or zip from memory)
4. zip
5. upload (pydrive)
6. if all partitions uploaded, import all and compile
7. up load compilation zip
8. if deletion desired, clear snapshots.


**Helpers:**
1. Upload helper
2. Import helper
3. compilation helper
4. deletion helper



```sh
# init & activate env
python3.10 -m venv .venv/src
source .venv/src/bin/activate

# install deps
pip install -r requirements.txt

# run
python ... .py
```


Note: bump requirements
```sh
pip freeze >| requirements.txt
```
