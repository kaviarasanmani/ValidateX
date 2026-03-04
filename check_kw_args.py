import ast
import os
import sys

errs = []
for dirpath, _, filenames in os.walk('tests/unit'):
    for fn in filenames:
        if not fn.endswith('.py'):
            continue
        filepath = os.path.join(dirpath, fn)
        src = open(filepath, 'r', encoding='utf-8').read()
        try:
            tree = ast.parse(src)
        except Exception as e:
            errs.append(f"SyntaxError in {filepath}: {e}")
            continue
            
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, 'id', '').startswith('Expect'):
                # Check keywords passed
                kwarg_names = [k.arg for k in node.keywords if k.arg is not None]
                invalid = [k for k in kwarg_names if k not in ['column', 'kwargs', 'meta']]
                if invalid:
                    func_id = getattr(node.func, 'id', '')
                    errs.append(f"{filepath}:{node.lineno}: {func_id} called with invalid args: {invalid}")

if errs:
    for e in errs:
        print(e)
    sys.exit(1)
else:
    print('All expectations correctly instantiated.')
    sys.exit(0)
