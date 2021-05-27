from setuptools import setup, find_packages

setup(
     name='snowbim',
     version='1.0.7',
     author='datnguye',
     author_email='datnguyen.it09@gmail.com',
     packages=find_packages(),
     url='https://github.com/datnguye/snowbim',
     license='MIT',
     description='A package to refresh power bi model .bim file',
     long_description_content_type="text/markdown",
     long_description=open('README.md').read(),
     install_requires=[
        'pyyaml==5.4.1',
        'snowflake-connector-python==2.4.3'
     ],
     python_requires='>=3.7.5',
     entry_points = {
        'console_scripts': [
            'snowbim = snowbim.__main__:main'
        ],
    }
)