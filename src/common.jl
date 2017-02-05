module Common

export capitalize

capitalize(s::String) = string(uppercase(s[1]), s[2:end])

end
