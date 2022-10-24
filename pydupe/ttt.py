# %%
import pathlib
p = pathlib.PurePath('/etc/passwd')
a = pathlib.Path('/etc')
b = pathlib.Path('/usr')
print(p.is_relative_to(a))

print(p.is_relative_to(b))

# %%
import pathlib
p = pathlib.Path("/home/chris/.bashrc")
filelist = p.rglob('*')
i = '/.' in str(p)
# %%
