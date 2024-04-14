from confluent_kafka.serialization import StringDeserializer, SerializationError, Deserializer

class HybridDeserializer(Deserializer):
    def __init__(self):
        self.string_deserializer = StringDeserializer('utf_8')

    def __call__(self, value, ctx):
        print(f"Raw key bytes: {value}")  # Add this line to log raw key data

        if value is None:
            return None

        # Try to decode as a UTF-8 string first
        try:
            return self.string_deserializer(value, ctx)
        except UnicodeDecodeError:
            # If the UTF-8 decoding fails, attempt to interpret as an integer
            try:
                return int.from_bytes(value, byteorder='big')
            except ValueError as e:
                raise SerializationError(f"Could not decode as UTF-8 or interpret as integer: {e}")

        # Catch any other unexpected errors
        except Exception as e:
            raise SerializationError(f"Unexpected error in HybridDeserializer: {e}")
