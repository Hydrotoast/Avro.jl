using Avro.Schemas

const PRIMITIVE_EXAMPLES = [
    ["\"$type_name\"" for type_name in PRIMITIVE_TYPES];
    ["{\"type\": \"$type_name\"}" for type_name in PRIMITIVE_TYPES]
    ]

const RECORD_EXAMPLES = [
    """
    {
        "type": "record",
        "name": "LongList",
        "aliases": ["LinkedLongs"],
        "fields" : [
            {
                "name": "value",
                "type": "long"
            },
            {
                "name": "next",
                "type": ["null", "LongList"]
            }
        ]
    }
    """
    ]

ENUM_EXAMPLES = [
    """
    {
        "type": "enum",
        "name": "Suit",
        "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
    }
    """
    ]

ARRAY_EXAMPLES = [
    """
    {
        "type": "array",
        "items": "string"
    }
    """
    ]

MAP_EXAMPLES = [
    """
    {
        "type": "map",
        "values": "long"
    }
    """
    ]

UNION_EXAMPLES = [
    "[\"null\", \"string\"]"
    ]

FIXED_EXAMPLES = [
    """
    {
        "type": "fixed",
        "name": "md5",
        "size": 256
    }
    """
    ]

@testset "Parsing schemas" begin
    @testset "Primitive" for example in PRIMITIVE_EXAMPLES
        Avro.parse(example)
    end

    @testset "Record" for example in RECORD_EXAMPLES
        Avro.parse(example)
    end

    @testset "Enum" for example in ENUM_EXAMPLES
        Avro.parse(example)
    end

    @testset "Array" for example in ARRAY_EXAMPLES
        Avro.parse(example)
    end

    @testset "Map" for example in MAP_EXAMPLES
        Avro.parse(example)
    end

    @testset "Union" for example in UNION_EXAMPLES
        Avro.parse(example)
    end

    @testset "Fixed" for example in FIXED_EXAMPLES
        Avro.parse(example)
    end
end

@testset "Schema equality" begin
    @testset "Primitive" begin
        @test Schemas.NULL == NullSchema()
        @test Schemas.NULL == Schemas.PrimitiveSchema("null")

        @test Schemas.BOOLEAN == BooleanSchema()
        @test Schemas.BOOLEAN == Schemas.PrimitiveSchema("boolean")

        @test Schemas.INT == IntSchema()
        @test Schemas.INT == Schemas.PrimitiveSchema("int")

        @test Schemas.LONG == LongSchema()
        @test Schemas.LONG == Schemas.PrimitiveSchema("long")
        
        @test Schemas.FLOAT == FloatSchema()
        @test Schemas.FLOAT == Schemas.PrimitiveSchema("float")

        @test Schemas.DOUBLE == DoubleSchema()
        @test Schemas.DOUBLE == Schemas.PrimitiveSchema("double")

        @test Schemas.BYTES == BytesSchema()
        @test Schemas.BYTES == Schemas.PrimitiveSchema("bytes")

        @test Schemas.STRING == StringSchema()
        @test Schemas.STRING == Schemas.PrimitiveSchema("string")
    end
end
