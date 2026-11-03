/* The board's memory map, which the linker script of `cortex-m-rt` reads: set the origins and
   lengths from the board's datasheet. */
MEMORY
{
  FLASH : ORIGIN = 0x08000000, LENGTH = 256K
  RAM   : ORIGIN = 0x20000000, LENGTH = 32K
}
