//! Frozen deterministic random algorithms used by every trace.

const SPLITMIX64_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// Canonical SplitMix64 output mixer.
pub const fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Fixed SplitMix64 generator. This algorithm is part of the trace contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(SPLITMIX64_GAMMA);
        mix64(self.state)
    }

    /// Samples uniformly from `0..upper` using rejection rather than modulo
    /// reduction. Callers only pass validated, non-zero configuration bounds.
    pub fn uniform_below(&mut self, upper: u64) -> u64 {
        assert!(upper > 0, "uniform upper bound must be non-zero");
        let rejection_threshold = upper.wrapping_neg() % upper;
        loop {
            let candidate = self.next_u64();
            if candidate >= rejection_threshold {
                return candidate % upper;
            }
        }
    }
}

/// Returns a deterministic Fisher-Yates permutation of `0..length`.
pub fn deterministic_permutation(length: usize, seed: u64) -> Vec<u64> {
    let mut values: Vec<u64> = (0..length)
        .map(|index| u64::try_from(index).expect("trace length must fit u64"))
        .collect();
    let mut random = SplitMix64::new(seed);
    for index in (1..length).rev() {
        let upper = u64::try_from(index + 1).expect("permutation bound must fit u64");
        let replacement = usize::try_from(random.uniform_below(upper))
            .expect("sampled permutation index must fit usize");
        values.swap(index, replacement);
    }
    values
}
