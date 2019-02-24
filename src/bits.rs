const MAX_INDEX: usize = 127;

#[derive(Clone)]
pub struct Bits {
    bits: u128,
}

impl Bits {
    pub fn new() -> Bits {
        Bits { bits: 0 }
    }

    pub fn from_vec(vec: Vec<i32>) -> Bits {
        let mut bits: u128 = 0;
        for interest in vec {
            bits |= (1 as u128) << interest;
        }
        Bits { bits }
    }

    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }

    pub fn contains(&self, index: i32) -> bool {
        (self.bits >> index as usize) & 1 != 0
    }

    pub fn contains_all(&self, other: &Bits) -> bool {
        if other.bits == 0 {
            unimplemented!();
        }
        (self.bits & other.bits) == other.bits
    }

    pub fn contains_any(&self, other: &Bits) -> bool {
        if other.bits == 0 {
            unimplemented!();
        }
        (self.bits & other.bits) != 0
    }

    pub fn count(&self) -> u32 {
        self.bits.count_ones()
    }

    pub fn count_common(&self, other: &Bits) -> u32 {
        (self.bits & other.bits).count_ones()
    }
}

impl<'a> IntoIterator for &'a Bits {
    type Item = i32;
    type IntoIter = BitsIntoIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitsIntoIterator {
            bits: &self,
            index: 1,
        }
    }
}

pub struct BitsIntoIterator<'a> {
    bits: &'a Bits,
    index: usize,
}

impl<'a> Iterator for BitsIntoIterator<'a> {
    type Item = i32;

    fn next(&mut self) -> Option<i32> {
        while self.index <= MAX_INDEX {
            let rest = self.bits.bits >> self.index;
            let bit = (rest & 1) as i32;
            if rest == 0 {
                self.index = MAX_INDEX + 1; // не дало эффекта
            } else {
                self.index += 1;
            }
            if bit != 0 {
                return Some(self.index as i32 - 1);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(MAX_INDEX + 1 - self.index))
    }
}

impl std::fmt::Debug for Bits {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let vec: Vec<i32> = self.into_iter().collect();
        vec.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bits() {
        {
            let bits = Bits::new();
            assert_eq!(bits.into_iter().collect::<Vec<i32>>(), Vec::<i32>::new());
            assert_eq!(bits.count(), 0);
        }
        {
            let bits = Bits::from_vec(Vec::new());
            assert_eq!(bits.into_iter().collect::<Vec<i32>>(), Vec::<i32>::new());
            assert_eq!(bits.count(), 0);
        }
        {
            let bits = Bits::from_vec(vec!(1, 3, 127));
            assert_eq!(bits.into_iter().collect::<Vec<i32>>(), vec!(1, 3, 127));
            assert_eq!(bits.count(), 3);
            assert_eq!(bits.contains(1), true);
            assert_eq!(bits.contains(2), false);
            assert_eq!(bits.contains(3), true);
            assert_eq!(bits.contains(127), true);
            assert_eq!(bits.contains_all(&Bits::from_vec(vec!(1, 127))), true);
            assert_eq!(bits.contains_all(&Bits::from_vec(vec!(1, 3, 127))), true);
            assert_eq!(bits.contains_all(&Bits::from_vec(vec!(1, 3, 5, 127))), false);
            assert_eq!(bits.contains_all(&Bits::from_vec(vec!(1, 5, 127))), false);
            assert_eq!(bits.contains_any(&Bits::from_vec(vec!(1, 127))), true);
            assert_eq!(bits.contains_any(&Bits::from_vec(vec!(2, 5))), false);
        }
        {
            let bits = Bits::from_vec(vec!(1, 3, 127));
            bits.into_iter().for_each(|i| {
                debug!("{}", i);
            });
        }
    }
}
