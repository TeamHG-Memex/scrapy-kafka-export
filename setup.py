from setuptools import setup


setup(
    name='scrapy-kafka-export',
    version='0.1.0',
    packages=['scrapy_kafka_export'],
    install_requires=[
        'scrapy',
        'kafka-python',
        'retrying'
    ],
    license='MIT license',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)