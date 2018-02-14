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
        name='glemon',
        author='黄涛',
        author_email='hunto@163.com',
        description='glemon',
        install_requires=requires,
        long_description='glemon',
        entry_points={'console_scripts':console_scripts,
                       'gui_scripts':gui_scripts},
        url='https://github.com/huangtao-sh/lemon.git',
        license='GPL',
        )
