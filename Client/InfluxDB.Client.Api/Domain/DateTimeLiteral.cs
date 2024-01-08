/* 
 * InfluxDB OSS API Service
 *
 * The InfluxDB v2 API provides a programmatic interface for all interactions with InfluxDB. Access the InfluxDB API using the `/api/v2/` endpoint. 
 *
 * OpenAPI spec version: 2.0.0
 * 
 * Generated by: https://github.com/openapitools/openapi-generator.git
 */

using System;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using NodaTime;

namespace InfluxDB.Client.Api.Domain
{
    /// <summary>
    /// Represents an instant in time with nanosecond precision using the syntax of golang&#39;s RFC3339 Nanosecond variant.
    /// </summary>
    [DataContract]
    public partial class DateTimeLiteral : Expression, IEquatable<DateTimeLiteral>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DateTimeLiteral" /> class with <see cref="DateTime"/>&#39;s ticks precision.
        /// </summary>
        /// <param name="type">Type of AST node.</param>
        /// <param name="value"><see cref="DateTime"/> value.</param>
        public DateTimeLiteral(string type = default, DateTime? value = default) : base()
        {
            Type = type;
            Value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DateTimeLiteral" /> class with nanosecond precision.
        /// </summary>
        /// <param name="type">Type of AST node.</param>
        /// <param name="value"><see cref="Instant"/> value.</param>
        public DateTimeLiteral(string type = default, Instant? value = default) : base()
        {
            Type = type;
            ValueInstant = value;
        }

        /// <summary>
        /// Type of AST node.
        /// </summary>
        /// <value>Type of AST node.</value>
        [DataMember(Name = "type", EmitDefaultValue = false)]
        public string Type { get; set; }

        /// <summary>
        /// Gets or sets the value as a <see cref="DateTime"/>.
        /// </summary>
        public DateTime? Value
        {
            get => ValueInstant?.ToDateTimeUtc();
            set => ValueInstant = value.HasValue ? Instant.FromDateTimeUtc(value.Value.ToUniversalTime()) : null;
        }

        /// <summary>
        /// Gets or sets the value as an <see cref="Instant"/>.
        /// </summary>
        [DataMember(Name = "value", EmitDefaultValue = false)]
        public Instant? ValueInstant { get; set; }

        /// <summary>
        /// Returns the string presentation of the object
        /// </summary>
        /// <returns>String presentation of the object</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("class DateTimeLiteral {\n");
            sb.Append("  ").Append(base.ToString().Replace("\n", "\n  ")).Append("\n");
            sb.Append("  Type: ").Append(Type).Append("\n");
            sb.Append("  Value: ").Append(Value).Append("\n");
            sb.Append("}\n");
            return sb.ToString();
        }

        /// <summary>
        /// Returns the JSON string presentation of the object
        /// </summary>
        /// <returns>JSON string presentation of the object</returns>
        public override string ToJson()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }

        /// <summary>
        /// Returns true if objects are equal
        /// </summary>
        /// <param name="input">Object to be compared</param>
        /// <returns>Boolean</returns>
        public override bool Equals(object input)
        {
            return Equals(input as DateTimeLiteral);
        }

        /// <summary>
        /// Returns true if DateTimeLiteral instances are equal
        /// </summary>
        /// <param name="input">Instance of DateTimeLiteral to be compared</param>
        /// <returns>Boolean</returns>
        public bool Equals(DateTimeLiteral input)
        {
            if (input == null)
            {
                return false;
            }

            return base.Equals(input) &&
                   (
                       Type == input.Type ||
                       Type != null && Type.Equals(input.Type)
                   ) && base.Equals(input) &&
                   (
                       ValueInstant == input.ValueInstant ||
                       ValueInstant != null && ValueInstant.Equals(input.ValueInstant)
                   );
        }

        /// <summary>
        /// Gets the hash code
        /// </summary>
        /// <returns>Hash code</returns>
        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                var hashCode = base.GetHashCode();

                if (Type != null)
                {
                    hashCode = hashCode * 59 + Type.GetHashCode();
                }

                if (ValueInstant != null)
                {
                    hashCode = hashCode * 59 + ValueInstant.GetHashCode();
                }

                return hashCode;
            }
        }
    }
}