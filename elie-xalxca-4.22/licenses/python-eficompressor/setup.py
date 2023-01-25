## @file
# package and install PyEfiCompressor extension
#
#  Copyright (c) 2008, Intel Corporation. All rights reserved.<BR>
#
#  This program and the accompanying materials
#  are licensed and made available under the terms and conditions of the BSD License
#  which accompanies this distribution.  The full text of the license may be found at
#  http://opensource.org/licenses/bsd-license.php
#
#  THE PROGRAM IS DISTRIBUTED UNDER THE BSD LICENSE ON AN "AS IS" BASIS,
#  WITHOUT WARRANTIES OR REPRESENTATIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED.
#

##
# Import Modules
#
import setuptools

setuptools.setup(
    name="EfiCompressor",
    version="0.7",
    description="Compress and decompress files using the EFI compression format",
    author="Intel TianoCore Project",
    author_email="edk2-devel@lists.sourceforge.net",
    maintainer="Matthew Garrett",
    maintainer_email="mjg59@srcf.ucam.org",
    ext_modules=[
        setuptools.Extension(
            'EfiCompressor',
            sources=[
                'Decompress.c',
                'EfiCompress.c',
                'TianoCompress.c',
                'EfiCompressor.c'
            ],
            include_dirs=[
                'Include',
            ],
        )
    ],
    classifiers=[
        'License :: OSI Approved :: BSD License',
    ]
)

