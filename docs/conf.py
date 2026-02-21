import os
import sys
sys.path.insert(0, os.path.abspath('..'))

project = 'ValidateX'
copyright = '2026, Kaviarasan Mani'
author = 'Kaviarasan Mani'
release = '1.0.1'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'myst_parser',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'sphinx_rtd_theme'
# html_static_path = ['_static']
