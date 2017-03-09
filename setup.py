from setuptools import setup

setup(
    name='RabbitMQEvtMgrPlugin',
    version='1.0',
    packages=['RabbitMQEvtMgrPlugin'],
    install_requires=
    [
        'Coronado',
        'pika',
        'EventManagerPlugin'
    ],
    author='Mukul Majmudar',
    author_email='mukul@curecompanion.com',
    description='RabbitMQ-based event manager plugin for Coronado')
