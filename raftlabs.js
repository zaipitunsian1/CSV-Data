const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Inputs
const readData = async (authorsList, booksList, magazinesList) => {
	// Populate authors
	const readAuthorsPromise = new Promise((resolve, reject) => {
		const authorsList = [];
		fs.createReadStream('authors.csv')
			.pipe(csv({separator: ';', headers: ['email', 'firstname', 'lastname'], skipLines: 1}))
			.on('data', (author) => {
				authorsList.push(author);
			})
			.on('end', () => {
				resolve(authorsList);
			});
	});

	// Populate Books
	const readBooksPromise = new Promise ((resolve, reject) => {
		const booksList = [];
		fs.createReadStream('books.csv')
			.pipe(csv({separator: ';', headers: ['title', 'isbn', 'authors', 'description'], skipLines: 1}))
			.on('data', (book) => {
				booksList.push({...book, authors: book.authors.split(',')});
			})
			.on('end', () => {
				resolve(booksList);
			});
	});

	// Populate Magazines
	const readMagazinesPromise = new Promise((resolve, reject) => {
		const magazinesList = [];
		fs.createReadStream('magazines.csv')
			.pipe(csv({separator: ';', headers: ['title', 'isbn', 'authors', 'publishedAt'], skipLines: 1}))
			.on('data', (magazine) => {
				magazinesList.push({...magazine, authors: magazine.authors.split(',')});
			})
			.on('end', () => {
				resolve(magazinesList);
			});
	});

	const result = await Promise.all([readAuthorsPromise, readBooksPromise, readMagazinesPromise]);
	return result;
};

// create look up data structures
const processAuthorsData = (authorsList) => authorsList.reduce((authorsMap, author) => {
	authorsMap[author.email] = author;
	return authorsMap;
}, {});
const processBooksData = (booksList) => booksList.reduce((booksMap, book) => {
	booksMap[book.isbn] = book;
	return booksMap;
}, {});
const processMagazinesData = (magazinesList) => magazinesList.reduce((magazinesMap, magazine) => {
	magazinesMap[magazine.isbn] = magazine;
	return magazinesMap;
}, {});


// Returns books and mags with details
const getBooksAndMagsWithDetails = (booksList, magazinesList, authors) => {
	const booksAndMags = [...booksList, ...magazinesList].map((item) => {
		const authorsListWithDetails = item.authors.map((authorEmail) => authors[authorEmail]);
		return {...item, authors: authorsListWithDetails};
	});
	return booksAndMags;
}

// Print Books and Magazines
const printBooksAndMagazines = (booksList, magazinesList, authors) => {
	const booksAndMags = getBooksAndMagsWithDetails(booksList, magazinesList, authors);
	console.log(JSON.stringify(booksAndMags, null, 2));
};

// Find book or magazine
const findBookOrMag = (books, magazines, isbn) => {
	return isbn in books ? books[isbn] : magazines[isbn];
};

// Find books and magazines by a given authorEmail
const findBooksAndMagsByAuthor = (authorEmail, booksList, magazinesList) => {
	return [...booksList, ...magazinesList].filter((item) => item.authors.includes(authorEmail));
}

const run = async () => {
	// Steps
	let authorsList = [];
	let booksList = [];
	let magazinesList = [];

	/* Question 1 */
	[authorsList, booksList, magazinesList] = await readData(authorsList, booksList, magazinesList);

	// Lookup data structures
	let authors = processAuthorsData(authorsList);
	let books = processBooksData(booksList);
	let magazines = processMagazinesData(magazinesList);

	printBooksAndMagazines(booksList, magazinesList, authors);

	const sampleBook = findBookOrMag(books, magazines, "2221-5548-8585");
	console.log(JSON.stringify(sampleBook, null, 2));

	const sampleBooksAndMagsByAuthor = findBooksAndMagsByAuthor("null-lieblich@echocat.org", booksList, magazinesList);
	console.log(JSON.stringify(sampleBooksAndMagsByAuthor, null, 2));

	// sort
	const booksAndMags = getBooksAndMagsWithDetails(booksList, magazinesList, authors);
	const booksAndMagsSorted = booksAndMags.sort((itemA, itemB) => itemA.title.localeCompare(itemB.title));
	console.log(JSON.stringify(booksAndMagsSorted, null, 2));



	/* Adding a book and a Magazine */
	const newBook = {
		title: 'The Immortals of Meluha',
		isbn: '9485-0345-3712',
		authors: [ 'amish_trivedi@gmail.com' ],
		description: 'All about Shiva Triology'
	};

	const newMag = {
		title: 'India Today Magazine',
		isbn: '2832-2832-4854',
		authors: [ 'pranil@gmail.com' ],
		publishedAt: '12.02.4567'
	};


	booksList.push(newBook);
	books[newBook.isbn] = newBook;

	magazinesList.push(newMag);
	magazines[newMag.isbn] = newMag;

	const booksCSVWriter = createCsvWriter({
		path: 'outputBooks.csv',
		header: [
			{id: 'title', title: 'title'},
			{id: 'isbn', title: 'isbn'},
			{id: 'authors', title: 'authors'},
			{id: 'description', title: 'description'}
		],
		fieldDelimiter: ';'
	});

	const booksForCSV = booksList.map((book) => ({...book, authors: book.authors.join(',')}));
	booksCSVWriter.writeRecords(booksForCSV);

	const magsCSVWriter = createCsvWriter({
		path: 'outputMagazines.csv',
		header: [
			{id: 'title', title: 'title'},
			{id: 'isbn', title: 'isbn'},
			{id: 'authors', title: 'authors'},
			{id: 'publishedAt', title: 'publishedAt'}
		],
		fieldDelimiter: ';'
	});

	const magsForCSV = magazinesList.map((mag) => ({...mag, authors: mag.authors.join(',')}));
	magsCSVWriter.writeRecords(magsForCSV);


};
run();