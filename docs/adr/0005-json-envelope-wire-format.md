# Integration events travel as JSON in a standard envelope

Despite Kafka being the reference broker, we rejected Avro + Schema Registry (couples the framework to Confluent tooling the broker port deliberately avoids) and protobuf (codegen toolchain in the template). Integration events serialize as JSON inside a framework envelope carrying event id, type, version, occurred-at, and trace context; schema discipline lives in the versioned Go contract types (V1/V2), not a registry. Revisit if polyglot consumers become a stated goal.
