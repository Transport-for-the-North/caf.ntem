![Transport for the North Logo](https://github.com/transport-for-the-north/caf.ntem/blob/main/docs/TFN_Landscape_Colour_CMYK.png)

<h1 align="center">CAF.ntem</h1>

<p align="center">
<a href="https://pypi.org/project/caf.ntem/">
  <img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/caf.ntem.svg?style=flat-square">
</a>
<a href="https://pypi.org/project/caf.ntem/">
  <img alt="Latest release" src="https://img.shields.io/github/release/transport-for-the-north/caf.ntem.svg?style=flat-square&maxAge=86400">
</a>
<a href="https://anaconda.org/conda-forge/caf.ntem">
  <img alt="Conda" src="https://img.shields.io/conda/v/conda-forge/caf.ntem?style=flat-square&logo=condaforge">
</a>
<a href="https://app.codecov.io/gh/transport-for-the-north/caf.ntem">
  <img alt="Coverage" src="https://img.shields.io/codecov/c/github/transport-for-the-north/caf.ntem.svg?branch=main&style=flat-square&logo=CodeCov">
</a>
<a href="https://github.com/transport-for-the-north/caf.ntem/actions?query=event%3Apush">
  <img alt="Testing Badge" src="https://img.shields.io/github/actions/workflow/status/transport-for-the-north/caf.ntem/tests.yml?style=flat-square&logo=GitHub&label=Tests">
</a>
<a href='https://cafntem.readthedocs.io/en/stable/?badge=stable'>
  <img alt='Documentation Status' src="https://img.shields.io/readthedocs/cafntem?style=flat-square&logo=readthedocs">
</a>
<a href="https://github.com/psf/black">
  <img alt="code style: black" src="https://img.shields.io/badge/code%20format-black-000000.svg">
</a>
</p>

CAF package for extracting and analysing NTEM data.



## Common Analytical Framework

This package sits within the [Common Analytical Framework (CAF)](https://transport-for-the-north.github.io/caf_homepage/intro.html),
which is a collaboration between transport bodies in the UK to develop and maintain commonly used
transport analytics and appraisal tools.

## Maintainers

- Kieran Fishwick (Kieran-Fishwick-TfN)
- Matt Buckley (MattBuckley-TfN)

## Contributing

CAF.ntem happily accepts contributions.

The best way to contribute to this project is to go to the [issues tab](https://github.com/transport-for-the-north/caf.ntem/issues)
and report bugs or submit a feature request. This helps CAF.ntem become more
stable and full-featured. Please check the closed bugs before submitting a bug report to see if your
question has already been answered.

Please see our [contribution guidelines](https://github.com/Transport-for-the-North/.github/blob/main/CONTRIBUTING.rst)
for details on contributing to the codebase or documentation.

## Documentation

Documentation is created using [Sphinx](https://www.sphinx-doc.org/en/master/index.html) and is hosted online at
[cafntem.readthedocs](https://cafntem.readthedocs.io/en/stable/).

The documentation can be built locally once all the docs requirements
([`docs/requirements.txt`](docs/requirements.txt)) are installed into your Python environment.

The provided make batch file, (inside the docs folder), allow for building the documentation in
various target formats. The command for building the documentation is `make {target}`
(called from within docs/), where `{target}` is the type of documentation format to build. A full
list of all available target formats can be seen by running the `make` command without any
arguments but the two most common are detailed below.

### HTML

The HTML documentation (seen on Read the Docs) can be built using the `make html` command, this
will build the web-based documentation and provide an index.html file as the homepage,
[`docs/build/html/index.html`](docs/build/html/index.html).

### PDF

The PDF documentation has some other requirements before it can be built as Sphinx will first
build a [LaTeX](https://www.latex-project.org/) version of the documentation and then use an
installed TeX distribution to build the PDF from those. If you already have a TeX distribution
setup then you can build the PDF with `make latexpdf`, otherwise follow the instructions below.

Installing LaTeX on Windows is best done using [MiKTeX](https://miktex.org/), as this provides a
simple way of handling any additional TeX packages. Details of other operating systems and TeX
distributions can be found on the [Getting LaTeX](https://www.latex-project.org/get/) page on
LaTeX's website.

MiKTeX provides an installer on its website [miktex.org/download](https://miktex.org/download),
which will run through the process of getting it installed and setup. In addition to MiKTeX
the specific process Sphinx uses for building PDFs is [Latexmk](https://mg.readthedocs.io/latexmk.html),
which is a Perl script and so requires Perl to be installed on your machine, this can be done with an
installer provided by [Strawberry Perl](https://strawberryperl.com/).

Once MiKTex and Perl are installed you are able to build the PDF from the LaTeX files, Sphinx
provides a target (latexpdf) which builds the LaTeX files then immediately builds the PDF. When
running `make latexpdf` MiKTeX may ask for permission to installed some required TeX packages.
Once the command has finished the PDF will be located at
[`docs/build/latex/cafntem.pdf`](docs/build/latex/cafntem.pdf).


## Credit

This project was created using the Common Analytical Framework cookiecutter template found here:
<https://github.com/Transport-for-the-North/cookiecutter-caf>

This project provides and alternate interface to the UK Department for Transport's National Trip End Model (NTEM) data which can be found here:
<https://www.data.gov.uk/dataset/11bc7aaf-ddf6-4133-a91d-84e6f20a663e/national-trip-end-model-ntem>
