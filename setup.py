import setuptools

with open('requirements.txt', 'r') as fh:
    requirements = fh.readlines()

for lnum, line in enumerate(requirements):
    if line[:3] == 'git':
        requirements.pop(lnum)

setuptools.setup(
    name="CeleryTestCase",
    version="1.0",
    author="Klemen Pukl",
    description="CeleryTestCase",
    long_description="CeleryTestCase",
    # long_description_content_type="text/x-rst",
    url="https://github.com/Brontes/CeleryTestCase",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=requirements,
    python_requires='>=3.7',
    license='BSD-3-Clause',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Framework :: Django",
    ],
)
