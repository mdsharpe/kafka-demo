namespace Model
{
	public class Transaction 
	{
		public string Product { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public double Value { get; set; }
	}
}
