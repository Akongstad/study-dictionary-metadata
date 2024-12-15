def str_hash(z):
    """
    Compute a hash for the input string using a technique similar to the sqlite implementation.
    """
    # Example sqlite3UpperToLower array: Convert ASCII to lowercase (or identity for non-alphabetic characters).
    sqlite3UpperToLower = [i if i < 128 else 0 for i in range(256)]
    for i in range(ord("A"), ord("Z") + 1):
        sqlite3UpperToLower[i] = ord(chr(i).lower())

    h = 0
    for char in z:
        c = ord(char)
        h += sqlite3UpperToLower[c]
        h *= 0x9E3779B1  # Knuth multiplicative hashing constant
        h &= 0xFFFFFFFF  # Ensure h stays a 32-bit unsigned value
    return h


# Strings to hash
strings = [f"t_{i}" for i in range(100000)]

# Compute hashes for the strings
hashes = {s: str_hash(s) for s in strings}

seen = set()
collisions = 0

# Display the results
for s, h in hashes.items():
    if s in seen:
        collisions += 1
        print(f"Hash for '{s}': {h}")
    seen.add(h)
