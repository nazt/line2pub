from setuptools import setup

setup(
	name="csv2line",
	version="1.0",
	py_modules=["csv2line"],
	include_package_data=True,
	install_requires=["click", "tqdm", "pandas"],
	entry_points="""
        [console_scripts]
        csv2line=csv2line:cli
    """,
)
