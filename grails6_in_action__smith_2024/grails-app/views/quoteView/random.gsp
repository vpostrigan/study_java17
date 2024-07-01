<html>
<head>
    <title>Random Quote</title>
</head>
<body>
    <ul id="menu">
        <li>
            <g:link action="ajaxRandom" update="quoteView">
                Next Quote
            </g:link>
        </li>
        <li>
            <g:link action="index">
                Admin
            </g:link>
        </li>
    </ul>

    <div id="quote">
        <q>${quote.content}</q>
        <p>${quote.author}</p>
    </div>
</body>
</html>
