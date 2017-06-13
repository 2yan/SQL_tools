from setuptools import setup

setup(name='sql_tools',
      version='0.932',
      description='SQL Tools - For Data Analysts!',
      url='https://github.com/2yan/ryan_sql',
      author='Ryan Francis',
      author_email='2yan@outlook.com',
      license='MIT',
      packages=['sql_tools'],
      install_requires= ['pandas', 'ryan_tools'],
      zip_safe=False)
