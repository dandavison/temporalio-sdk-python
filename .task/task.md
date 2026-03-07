I'd like to create a `async with Server(...)` context manager, to pair with `async with Worker`. I'm
thinking that this will share with the tests the machinery they already have for downloading the dev
server.