﻿using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;

namespace Npgsql.FrontendMessages
{
    class DescribeMessage : SimpleFrontendMessage
    {
        /// <summary>
        /// The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
        /// </summary>
        internal string Name { get; set; }
        /// <summary>
        /// Whether to describe a statement or a portal
        /// </summary>
        internal DescribeType DescribeType { get; set; }

        const byte Code = (byte)'D';

        internal DescribeMessage(DescribeType type, string name)
        {
            DescribeType = type;
            Name = name;
        }

        internal override int Length { get { return 1 + 4 + 1 + Name.Length; } }

        internal override void Write(NpgsqlBuffer buf)
        {
            Contract.Requires(Name != null && Name.All(c => c < 128));

            buf.WriteByte(Code);
            buf.WriteInt32(Length);
            buf.WriteByte((byte)DescribeType);
            buf.WriteBytesNullTerminated(Encoding.ASCII.GetBytes(Name));
        }

        public override string ToString()
        {
            return String.Format("[Describe({0}={1})]", DescribeType, Name);
        }
    }
}
