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

info("Test parsing primitive examples")
for example in PRIMITIVE_EXAMPLES
    parse_json(example)
end

info("Test parsing record examples")
for example in RECORD_EXAMPLES
    parse_json(example)
end

info("Test parsing enum examples")
for example in ENUM_EXAMPLES
    parse_json(example)
end

info("Test parsing array examples")
for example in ARRAY_EXAMPLES
    parse_json(example)
end

info("Test parsing map examples")
for example in MAP_EXAMPLES
    parse_json(example)
end

info("Test parsing union examples")
for example in UNION_EXAMPLES
    parse_json(example)
end

info("Test parsing fixed examples")
for example in FIXED_EXAMPLES
    parse_json(example)
end
