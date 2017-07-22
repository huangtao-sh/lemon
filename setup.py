#!/usr/bin/env python3
from orange import setup
requires=['motor',
          ]
console_scripts=[
        # 'cmdname=package:function',
                 ]
gui_scripts=[
        # 'cmdname=package:function',
                 ]
setup(
        name='lemon',
        author='黄涛',
        author_email='hunto@163.com',
        platforms='any',
        description='lemon',
        install_requires=requires,
        long_description='lemon',
        entry_points={'console_scripts':console_scripts,
                       'gui_scripts':gui_scripts},
        url='https://github.com/huangtao-sh/lemon.git',
        license='GPL',
        )
