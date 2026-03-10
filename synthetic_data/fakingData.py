from faker import Faker 

fake = Faker()

class Person:
    def __init__(self, first_name, last_name, age):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
    
    def __str__(self):
        return f"{self.first_name} {self.last_name}, Age: {self.age}"

person = Person(fake.first_name(), fake.last_name(), fake.random_int(min=18, max=80))

print(person)