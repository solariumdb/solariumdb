using Solarium.Engine;

namespace Solarium.Tests;

/// <summary>The credit that decides whether a waiting writer spins long: its start, its steps, and its threshold.</summary>
public sealed class WriterSpinCreditTests
{
    [Fact]
    public void A_new_credit_gives_the_long_spin_with_a_margin()
    {
        WriterSpinCredit credit = new();

        Assert.Equal(WriterSpinCredit.Start, credit.Value);
        Assert.True(credit.Value > 0);
        Assert.True(credit.LongSpinPays);
    }

    [Fact]
    public void Sixteen_probes_that_block_in_a_row_stop_the_long_spin_and_probes_in_time_bring_it_back()
    {
        WriterSpinCredit credit = new();

        for (int i = 0; i < 15; i++)
            credit.Record(inTime: false);
        Assert.True(credit.LongSpinPays);
        credit.Record(inTime: false);
        Assert.False(credit.LongSpinPays);

        credit.Record(inTime: true);
        Assert.True(credit.LongSpinPays);

        // Near 0, a block weighs three probes in time, less the decay.
        credit.Record(inTime: false);
        Assert.False(credit.LongSpinPays);
        credit.Record(inTime: true);
        credit.Record(inTime: true);
        Assert.False(credit.LongSpinPays);
        credit.Record(inTime: true);
        Assert.True(credit.LongSpinPays);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(13, true)]    // the most blocks where the long spin was faster
    [InlineData(20, true)]
    [InlineData(30, false)]
    [InlineData(40, false)]   // the fewest blocks where the long spin was slower
    [InlineData(100, false)]
    public void The_long_spin_pays_while_at_most_a_quarter_of_the_probes_block(int blocksPerHundred, bool pays)
    {
        WriterSpinCredit credit = new();
        const int probes = 20_000;

        for (int i = 0; i < probes; i++)
        {
            // Exactly blocksPerHundred blocks in any hundred probes in a row, spread out.
            credit.Record(inTime: i * blocksPerHundred % 100 >= blocksPerHundred);

            // After the first probes, the credit is near the level that the share of blocks sets.
            if (i >= probes / 2)
                Assert.Equal(pays, credit.LongSpinPays);
        }
    }

    [Fact]
    public void The_credit_stays_within_its_range_after_long_runs_of_one_outcome()
    {
        WriterSpinCredit credit = new();

        for (int i = 0; i < 100_000; i++)
            credit.Record(inTime: false);
        Assert.InRange(credit.Value, -3 * (1 << 27), -1);

        for (int i = 0; i < 100_000; i++)
            credit.Record(inTime: true);
        Assert.InRange(credit.Value, 1, 1 << 27);
    }
}
