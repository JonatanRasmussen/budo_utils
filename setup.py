from setuptools import setup, find_packages  #type: ignore[import]

# THIS IS ONLY A TEMPLATE FROM CHATGPT, IT IS NOT YET DONE
setup(
    name='budo',  # Replace with your project name
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'selenium',  # Add other dependencies here
        'webdriver-manager'
    ],
    entry_points={
        'console_scripts': [
            'your_command=your_module.your_script:main',  # If you want to create command-line scripts
        ],
    },
    author='Your Name',
    author_email='your.email@example.com',
    description='A brief description of your project',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/yourproject',  # Replace with your GitHub project URL
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
