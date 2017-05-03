|Build Status|

Proxo
=====

Extend protobuf message with custom methods properties and additional
attributes

TL;DR
-----

.. code:: python


    from proxo import MessageProxy, encode, decode

    class Person(MessageProxy):
        proto = addressbook_pb2.Person  # it can be more complex, like pattern matching, see below

        @property
        def firstname(self):
            return self.name.split(' ')[0]


    p = Person(name='Test Me')
    assert p.firstname == 'Test'
    assert decode(encode(p)) == p

Usage
-----

Given the addressbook protobuf definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: protobuf


    package tutorial;

    message Person {
      required string name = 1;
      required int32 id = 2;
      optional string email = 3;

      enum PhoneType {
        MOBILE = 0;
        HOME = 1;
        WORK = 2;
      }

      message PhoneNumber {
        required string number = 1;
        optional PhoneType type = 2 [default = HOME];
      }

      repeated PhoneNumber phone = 4;
    }

    message AddressBook {
      repeated Person person = 1;
    }

The traditional way
~~~~~~~~~~~~~~~~~~~

.. code:: python


    import addressbook_pb2

    person = addressbook_pb2.Person()
    person.id = 1234
    person.name = "John Doe"
    person.email = "jdoe@example.com"
    phone = person.phone.add()
    phone.number = "555-4321"
    phone.type = addressbook_pb2.Person.HOME

via Proxo.dict\_to\_protobuf
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python


    from proxo import dict_to_protobuf, protobuf_to_dict

    data = {'id': 124,
            'name': 'John Doe',
            'email': 'jdoe@example.com',
            'phone': {'number': '555-4321',
                      'type': 'HOME'}}

    proto = dict_to_protobuf(data, addressbook_pb2.Person)

    assert person == proto

    # converting back
    mapping = protobuf_to_dict(proto)
    mapping['phone']['number']
    mapping.phone.number  # using dot notation

    assert mapping == data

via extending Proxo.MessageProxy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python


    from proxo import MessageProxy, encode, decode

    # note that non defined types will be automatically proxied too

    class Person(MessageProxy):
        proto = addressbook_pb2.Person  # it can be more complex, like pattern matching, see below

        @property
        def firstname(self):
            return self.name.split(' ')[0]

        def call(self):
            try:
                print('calling {}'.format(self.firstname))
                do_voip_call(self.phone.number)
            except:
                print('failed calling {} on his/her {} number'.format(self.firstname,
                                                                      self.phone.type.lower))

    obj = Person(id=124, name='John Doe', phone={'number': '555-4321',
                                                 'type': 'HOME'})
    obj.phone.type = 'MOBILE'
    assert obj.firsname == 'John'

    proto = encode(obj)
    john = decode(proto)

    # lets bother him
    john.call()

More Complicated Example
------------------------

.. code:: python



    import operator

    from uuid import uuid4
    from functools import partial
    from proxo import MessageProxy


    class Scalar(MessageProxy):
        proto = mesos_pb2.Value.Scalar


    class Resource(MessageProxy):
        proto = mesos_pb2.Resource  # can be class


    class ScalarResource(Resource):
        proto = mesos_pb2.Resource(type=mesos_pb2.Value.SCALAR)  # or partially set instance

        def __init__(self, value=None, **kwargs):
            super(Resource, self).__init__(**kwargs)
            if value is not None:
                self.scalar = Scalar(value=value)

        def __cmp__(self, other):
            first, second = float(self), float(other)
            if first < second:
                return -1
            elif first > second:
                return 1
            else:
                return 0

        def __repr__(self):
            return "<{}: {}>".format(self.__class__.__name__, self.scalar.value)

        def __float__(self):
            return float(self.scalar.value)

        @classmethod
        def _op(cls, op, first, second):
            value = op(float(first), float(second))
            return cls(value=value)

        def __add__(self, other):
            return self._op(operator.add, self, other)

        def __radd__(self, other):
            return self._op(operator.add, other, self)

        def __sub__(self, other):
            return self._op(operator.sub, self, other)

        def __rsub__(self, other):
            return self._op(operator.sub, other, self)

        def __mul__(self, other):
            return self._op(operator.mul, self, other)

        def __rmul__(self, other):
            return self._op(operator.mul, other, self)

        def __truediv__(self, other):
            return self._op(operator.truediv, self, other)

        def __rtruediv__(self, other):
            return self._op(operator.truediv, other, self)

        def __iadd__(self, other):
            self.scalar.value = float(self._op(operator.add, self, other))
            return self

        def __isub__(self, other):
            self.scalar.value = float(self._op(operator.sub, self, other))
            return self


    class Cpus(ScalarResource):
        proto = mesos_pb2.Resource(name='cpus', type=mesos_pb2.Value.SCALAR)


    class Mem(ScalarResource):
        proto = mesos_pb2.Resource(name='mem', type=mesos_pb2.Value.SCALAR)


    class Disk(ScalarResource):
        proto = mesos_pb2.Resource(name='disk', type=mesos_pb2.Value.SCALAR)

.. |Build Status| image:: http://drone.lensa.com:8000/api/badges/kszucs/pandahouse/status.svg
   :target: http://drone.lensa.com:8000/kszucs/pandahouse
