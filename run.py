# from snowbim import snowbim
# changes = snowbim.get_schema_changes()
# print(changes)

from snowbim.engines import bimengine
r = bimengine.upgrade_bim(file_path='', out_path=None, changes=[])
print(type(r[0]))
print(type(r[1]))