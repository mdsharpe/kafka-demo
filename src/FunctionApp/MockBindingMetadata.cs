using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;

namespace FunctionApp
{
    internal class MockBindingMetadata : BindingMetadata
    {
        public override string Name => "bytez";

        public override string Type => typeof(byte[]).ToString();

        public override BindingDirection Direction => BindingDirection.In;
    }
}
