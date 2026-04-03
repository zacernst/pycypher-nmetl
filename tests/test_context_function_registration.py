"""Tests for Context function registration decorator.

This module tests the @context.cypher_function decorator that allows
registering Python functions for use in Cypher query evaluation.
"""

import pytest
from pycypher.relational_models import Context, RegisteredFunction


class TestRegisteredFunction:
    """Test the RegisteredFunction model."""

    def test_create_registered_function(self):
        """Test creating a RegisteredFunction instance."""

        def sample_func(x, y):
            return x + y

        reg_func = RegisteredFunction(
            name="sample_func",
            implementation=sample_func,
            arity=2,
        )

        assert reg_func.name == "sample_func"
        assert reg_func.implementation is sample_func
        assert reg_func.arity == 2

    def test_call_registered_function_correct_arity(self):
        """Test calling a registered function with correct number of arguments."""

        def add(x, y):
            return x + y

        reg_func = RegisteredFunction(name="add", implementation=add, arity=2)

        result = reg_func(3, 5)
        assert result == 8

    def test_call_registered_function_wrong_arity_too_few(self):
        """Test that calling with too few arguments raises ValueError."""

        def add(x, y):
            return x + y

        reg_func = RegisteredFunction(name="add", implementation=add, arity=2)

        with pytest.raises(
            ValueError,
            match="expects 2 arguments but 1 provided",
        ):
            reg_func(3)

    def test_call_registered_function_wrong_arity_too_many(self):
        """Test that calling with too many arguments raises ValueError."""

        def add(x, y):
            return x + y

        reg_func = RegisteredFunction(name="add", implementation=add, arity=2)

        with pytest.raises(
            ValueError,
            match="expects 2 arguments but 3 provided",
        ):
            reg_func(3, 5, 7)

    def test_call_registered_function_zero_arity(self):
        """Test calling a zero-argument function."""

        def get_constant():
            return 42

        reg_func = RegisteredFunction(
            name="get_constant",
            implementation=get_constant,
            arity=0,
        )

        result = reg_func()
        assert result == 42

    def test_call_registered_function_zero_arity_no_check(self):
        """Test that arity=0 allows calling without arity check."""

        def variadic_func(*args):
            return sum(args)

        # When arity is 0 (default), no arity checking is performed
        reg_func = RegisteredFunction(
            name="variadic_func",
            implementation=variadic_func,
            arity=0,
        )

        # Should work with any number of arguments
        assert reg_func(1, 2, 3) == 6
        assert reg_func() == 0


class TestContextFunctionDecorator:
    """Test the Context.cypher_function decorator."""

    def test_register_simple_function(self):
        """Test registering a simple function."""
        context = Context()

        @context.cypher_function
        def double(x):
            return x * 2

        assert "double" in context.cypher_functions
        assert isinstance(
            context.cypher_functions["double"],
            RegisteredFunction,
        )
        assert context.cypher_functions["double"].name == "double"
        assert context.cypher_functions["double"].arity == 1

    def test_call_registered_function_from_context(self):
        """Test calling a function registered in the context."""
        context = Context()

        @context.cypher_function
        def multiply(a, b):
            return a * b

        result = context.cypher_functions["multiply"](6, 7)
        assert result == 42

    def test_register_multiple_functions(self):
        """Test registering multiple functions in the same context."""
        context = Context()

        @context.cypher_function
        def add(x, y):
            return x + y

        @context.cypher_function
        def subtract(x, y):
            return x - y

        @context.cypher_function
        def multiply(x, y):
            return x * y

        assert len(context.cypher_functions) == 3
        assert "add" in context.cypher_functions
        assert "subtract" in context.cypher_functions
        assert "multiply" in context.cypher_functions

        assert context.cypher_functions["add"](10, 5) == 15
        assert context.cypher_functions["subtract"](10, 5) == 5
        assert context.cypher_functions["multiply"](10, 5) == 50

    def test_register_zero_argument_function(self):
        """Test registering a function with no arguments."""
        context = Context()

        @context.cypher_function
        def get_pi():
            return 3.14159

        assert context.cypher_functions["get_pi"].arity == 0
        assert context.cypher_functions["get_pi"]() == 3.14159

    def test_register_multi_argument_function(self):
        """Test registering a function with multiple arguments."""
        context = Context()

        @context.cypher_function
        def calculate_volume(length, width, height):
            return length * width * height

        assert context.cypher_functions["calculate_volume"].arity == 3
        assert context.cypher_functions["calculate_volume"](2, 3, 4) == 24

    def test_function_name_extracted_correctly(self):
        """Test that the function name is correctly extracted."""
        context = Context()

        @context.cypher_function
        def my_custom_function(x):
            return x**2

        assert "my_custom_function" in context.cypher_functions
        assert (
            context.cypher_functions["my_custom_function"].name
            == "my_custom_function"
        )

    def test_function_overwrite(self):
        """Test that registering a function with the same name overwrites the previous one."""
        context = Context()

        @context.cypher_function
        def test_func(x):
            return x * 2

        first_result = context.cypher_functions["test_func"](5)

        @context.cypher_function
        def test_func(x):
            return x * 3

        second_result = context.cypher_functions["test_func"](5)

        assert first_result == 10
        assert second_result == 15

    def test_function_with_complex_return_type(self):
        """Test registering functions that return complex types."""
        context = Context()

        @context.cypher_function
        def make_list(n):
            return list(range(n))

        @context.cypher_function
        def make_dict(key, value):
            return {key: value}

        assert context.cypher_functions["make_list"](5) == [0, 1, 2, 3, 4]
        assert context.cypher_functions["make_dict"]("name", "Alice") == {
            "name": "Alice",
        }

    def test_function_with_string_operations(self):
        """Test registering functions that work with strings."""
        context = Context()

        @context.cypher_function
        def uppercase(s):
            return s.upper()

        @context.cypher_function
        def concat(s1, s2):
            return s1 + s2

        assert context.cypher_functions["uppercase"]("hello") == "HELLO"
        assert (
            context.cypher_functions["concat"]("Hello, ", "World!")
            == "Hello, World!"
        )

    def test_arity_enforcement_after_registration(self):
        """Test that arity is enforced after registration."""
        context = Context()

        @context.cypher_function
        def strict_function(x, y):
            return x + y

        # Should work with correct number of arguments
        assert context.cypher_functions["strict_function"](1, 2) == 3

        # Should fail with wrong number of arguments
        with pytest.raises(ValueError, match="expects 2 arguments"):
            context.cypher_functions["strict_function"](1)

        with pytest.raises(ValueError, match="expects 2 arguments"):
            context.cypher_functions["strict_function"](1, 2, 3)

    def test_multiple_contexts_independent(self):
        """Test that multiple contexts maintain independent function registries."""
        context1 = Context()
        context2 = Context()

        @context1.cypher_function
        def func_in_context1(x):
            return x * 2

        @context2.cypher_function
        def func_in_context2(x):
            return x * 3

        assert "func_in_context1" in context1.cypher_functions
        assert "func_in_context1" not in context2.cypher_functions

        assert "func_in_context2" in context2.cypher_functions
        assert "func_in_context2" not in context1.cypher_functions

        assert context1.cypher_functions["func_in_context1"](5) == 10
        assert context2.cypher_functions["func_in_context2"](5) == 15

    def test_function_with_default_arguments(self):
        """Test registering functions with default arguments."""
        context = Context()

        # Function with default arguments - arity counts all parameters
        @context.cypher_function
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        # Arity should be 2 (total parameter count)
        assert context.cypher_functions["greet"].arity == 2

        # Should work with 2 arguments
        assert context.cypher_functions["greet"]("Alice", "Hi") == "Hi, Alice!"

    def test_lambda_function_registration(self):
        """Test that lambda functions can be registered."""
        context = Context()

        # Create a lambda and register it
        square = lambda x: x**2
        context.cypher_function(square)

        assert "<lambda>" in context.cypher_functions
        assert context.cypher_functions["<lambda>"](5) == 25

    def test_closure_function(self):
        """Test registering a function that uses closure."""
        context = Context()

        multiplier = 10

        @context.cypher_function
        def multiply_by_constant(x):
            return x * multiplier

        assert context.cypher_functions["multiply_by_constant"](5) == 50

        # Change the outer variable
        multiplier = 20
        # The function should still use the original value from closure
        assert context.cypher_functions["multiply_by_constant"](5) == 100
