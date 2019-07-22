import re
a = "4 star load"
c=re.match("(.*?) star load",a).group(1)
print(c)

