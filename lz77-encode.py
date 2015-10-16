# LZ77 Encoding
#
# Simple implementation of Lempel-Ziv 77 (a.k.a. "Sliding Window") coding
# as described in Section 13.4 of Cover & Thomas' "Information Theory".
#
# USAGE: python encode.py INPUT_STREAM
#
# EXAMPLE (from lectures):
#    $ python encode.py abbababbababbab
#    (0,a)
#    (0,b)
#    (1,1,1)
#    (1,3,2)
#    (1,5,10)
#
# AUTHOR: Mark Reid
# CREATED: 2013-10-13
import sys
import logging

###################################################################################
# Coding
def encode(stream):
    """Encode the input stream using a hard-coded window size"""
    window = ""     # Buffer of last MAX_WINDOW_SIZE symbols
    s = ""          # Suffix of stream to be coded
    n = 0           # Index into stream
    
    while n < len(stream):
       logging.info("----- n = " + str(n) + " | window = " + window + " | s = " + s)
       x = stream[n]
       
       logging.info("READ: x = " + x)
       
       if (window + s).find(s + x) < 0:
           # Suffix extended by x could not described by previously seen symbols.
           if s == "":
               # No previously seen substring so code x and add it to window
               code_symbol(x)
               window = grow(window, x)
           else:
               # Find number of symbols back that s starts
               i = lookback(window, s)
               code_pointer(i,s) 
               window = grow(window, s) 

               # Reset suffix and push back last symbol
               s = ""
               n -= 1
       else:
           # Append the last symbol to the search suffix 
           s += x
       
       # Increment the stream index
       n += 1

    # Stream finished, flush buffer
    logging.info("READ: <END>")
    if s != "":
        i = lookback(window,s) 
        code_pointer(i,s)

def code_symbol(x):
    """Write a single symbol out"""
    print "(0," + x + ")"

def code_pointer(i,s):
    """Write a pointer out"""
    print "(1," + str(i) + "," + str(len(s)) + ")"

###################################################################################
# Window management

# Hard-coded maximum window size
MAX_WINDOW_SIZE = 15

def lookback(window, s):
    """Find the lookback index for the suffix s in the given window"""
    return len(window) - (window + s).find(s)

def grow(window, x):
    """Update a window by adding x and keeping only MAX_WINDOW most recent symbols"""
    window += x
    if len(window) > MAX_WINDOW_SIZE:
        window = window[-MAX_WINDOW_SIZE:]   # Keep MAX_WINDOW last symbols

    return window 

###################################################################################
# Main

if __name__ == "__main__":
    # Uncomment line below to show encoding state
    # logging.basicConfig(level=logging.INFO)
    input_string = sys.argv.pop()
    encode(input_string)