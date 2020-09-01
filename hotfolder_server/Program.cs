using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Threading;
using System.Security.Cryptography;
using System.Collections;

namespace hotfolder {
	class Program {
		public static Mutex arg_mut = new Mutex(), catch_up_mut = new Mutex();
		public static Int64 n_threads_catching_up;
		static ArrayList thread_objects = new ArrayList();
		static TcpClient clt;
		static NetworkStream ns;
		static Int32 tcp_port = 1234;
		static string tcp_pass = null, ini_filepath = null;

		static void Main(string[] args) {
			StreamReader sr;
			string line, msg;
			string[] split_line;
			TcpListener lnr;
			StreamWriter nsw;
			UInt64 current_line;

			// Parse config file.
			try {
				sr = new StreamReader($"hotfolder.cfg");
			} catch(FileNotFoundException) {
				Console.WriteLine("Config file 'hotfolder.cfg' not found. Using port 1234.");
				goto start_without_config;
			} catch {
				Console.WriteLine("Failed to open 'hotfolder.cfg'. Using port 1234.");
				goto start_without_config;
			}
			current_line = 1;
			while((line = sr.ReadLine()) != null) {
				split_line = line.Trim().Split();
				if(split_line.Length == 2) {
					if(string.Compare(split_line[0], "port") == 0) {
						try {
							tcp_port = Int32.Parse(split_line[1]);
						} catch {
							Console.WriteLine("Error reading config file:");
							Console.WriteLine($"Line {current_line}: Malformed port number.");
						}
					} else if(string.Compare(split_line[0], "password") == 0) {
						tcp_pass = split_line[1];
					}
				}
				current_line++;
			}
			sr.Close();

		start_without_config:
			if(args.Length >= 2) {
				if(args[0] == "-mode" && (string.Compare(args[1].Substring(args[1].Length - 4), ".ini") == 0)) {
					ini_filepath = args[1];
					// Start counter at 1 to prevent any threads from executing watch callbacks until we have created all the threads.
					// Watch callbacks will wait (while n_threads_catching_up != 0).
					n_threads_catching_up = 1;
					try {
						sr = new StreamReader($"{ini_filepath}");
					} catch(FileNotFoundException) {
						Console.WriteLine($"Error: Failed to open script '{ini_filepath}': File not found.");
						return;
					} catch {
						Console.WriteLine($"Error: Failed to open script '{ini_filepath}'.");
						return;
					}
					while((line = sr.ReadLine()) != null) {
						arg_mut.WaitOne();
						split_line = line.Split(' ');
						arg_mut.ReleaseMutex();
						
						// For every line, start a thread and increment the counter atomically to mark that catch-up still needs to be done for this thread.
						// Also, don't forget to check if the result after incrementing is 0. If it is, overflow occured and the program should close.
						Interlocked.Increment(ref n_threads_catching_up);
						if(n_threads_catching_up == 0) {
							Console.WriteLine("Error: Too many threads spawned.");
							Environment.Exit(0);
						}

						thread_objects.Add(new Thread_Object(split_line));
					}
					sr.Close();
					// Decrement the counter atomically now that all threads have been created.
					// Now, when all threads are done with their catch-up phase, watch callbacks will be able to execute their jobs.
					Interlocked.Decrement(ref n_threads_catching_up);
				} else {
					// Start a single thread.
					thread_objects.Add(new Thread_Object(args));
				}
			} else {
				Console.WriteLine($"Error: Invalid parameter count.");
				exit_with_usage();
			}

			lnr = new TcpListener(IPAddress.Any, tcp_port);
			Console.WriteLine("Connection with client via TCP is now available.");
			while(true) {
				try {
					lnr.Start();
				} catch(SocketException ex) {
					if(ex.ErrorCode == 10048) Console.WriteLine($"Error: TCP port {tcp_port} is already being used.");
					else Console.WriteLine($"Error: Unknown socket exception. Socket error code: {ex.ErrorCode}. For more information about socket error codes, see https://docs.microsoft.com/en-us/windows/desktop/winsock/windows-sockets-error-codes-2.");
					Environment.Exit(0);
				}

				clt = lnr.AcceptTcpClient();
				lnr.Stop();
				ns = clt.GetStream();
				nsw = new StreamWriter(ns);

				if(n_threads_catching_up != 0) {
					// Send 'server still starting up' message.
					nsw.Write("w\u0004");
					nsw.Flush();
					goto close_connection;
				}

				// Handle password.
				if(tcp_pass != null) {
					// Send password request.
					nsw.Write("p\u0004");
					nsw.Flush();

					// Read password message and validate it.
					if(!password_correct()) goto close_connection;
				} else {
					// Send 'connected without password' message.
					nsw.Write("c\u0004");
					nsw.Flush();
				}

				// Handle TCP requests.
				while(read_message(out msg)) {
					if(String.Compare(msg, "l") == 0) list_transfers();
					else if(String.Compare(msg, "a") == 0) add_transfer();
					else if(String.Compare(msg, "r") == 0) remove_transfer();
				}

			close_connection:
				ns.Close();
				clt.Close();
			}
		}

		public static bool password_correct() {
			StreamWriter nsw;
			string msg;

			if(!read_message(out msg)) return false;

			nsw = new StreamWriter(ns);
			if(string.Compare(msg, tcp_pass) == 0) {
				nsw.Write("o\u0004"); // OK.
				nsw.Flush();
				return true;
			} else {
				nsw.Write("n\u0004"); // Not OK.
				nsw.Flush();
				return false;
			}
		}

		public static bool read_message(out string msg) {
			char c;

			msg = "";
			while(clt.Connected) {
				try {
					c = (char)ns.ReadByte();
				} catch {
					return false;
				}

				if(c == -1) continue;
				if(c == 4) return true;
				else msg += c;	
			}
			return false;
		}

		public static void list_transfers() {
			StreamWriter nsw;
			string msg = null;

			nsw = new StreamWriter(ns);
			for(Int32 i = 0; i < thread_objects.Count; i++) {
				msg += $"{((Thread_Object)thread_objects[i]).src} {((Thread_Object)thread_objects[i]).dst} " +
				       $"{((Thread_Object)thread_objects[i]).ftp_srv} {((Thread_Object)thread_objects[i]).ftp_user}\n";
			}
			msg += "\u0004";

			nsw.Write(msg);
			nsw.Flush();
		}

		public static void add_transfer() {
			string msg;
			string[] args;
			
			if(!read_message(out msg)) return;
			
			args = msg.Split(' ');

			Interlocked.Increment(ref n_threads_catching_up);
			thread_objects.Add(new Thread_Object(args));

			// Append addition to .ini file.
			if(ini_filepath != null) {
				File.AppendAllText(ini_filepath, $"\r\n{msg}");
			}
		}

		public static void remove_transfer() {
			string msg, line_to_copy, tmp_dir;
			string[] split_ini_filepath;
			Int32 i, j;
			StreamReader input_sr;
			StreamWriter output_sw;

			if(!read_message(out msg)) return;

			try {
				i = Int32.Parse(msg);
			} catch {
				return;
			}
			
			((Thread_Object)thread_objects[i]).watcher.EnableRaisingEvents = false;
			((Thread_Object)thread_objects[i]).trd.Abort();
			thread_objects.RemoveAt(i);

			// Save removal to .ini file.
			if(ini_filepath != null) {	
				tmp_dir = Path.GetTempPath();

				input_sr = new StreamReader(ini_filepath);
				split_ini_filepath = ini_filepath.Split('\\');
				output_sw = new StreamWriter($"{tmp_dir}\\{split_ini_filepath[split_ini_filepath.Length - 1]}");

				line_to_copy = input_sr.ReadLine();
				j = 0;
				while(line_to_copy != null) {
					if(i != j) output_sw.WriteLine(line_to_copy);
					line_to_copy = input_sr.ReadLine();
					j++;
				}

				input_sr.Close();
				output_sw.Close();

				File.Copy($"{tmp_dir}\\{split_ini_filepath[split_ini_filepath.Length - 1]}", ini_filepath, true);
			}
		}

		public static void exit_with_usage() {
			Console.WriteLine("Usage: hotfolder.exe -source <source> -destination <destination> [-mode <mode> -width <width> -height <height>] [-ftpserver <hostname> -ftpuser <username> -ftppass <password>]");
			Environment.Exit(0);
		}
	}

	class Thread_Object {
		enum Mode {
			COPY = 0,
			RESIZE,
			THUMB
		};
		public string src = null, dst = null, ftp_srv = null, ftp_user = null;
		public Thread trd;
		public FileSystemWatcher watcher;

		Mutex last_state_mut = new Mutex();
		string tmp_dir, ftp_pass = null, ftp_uri = null, last_state_path;
		int w = 0, h = 0;
		Mode mode = Mode.COPY;
		bool use_ftp = false;
		WebClient client;

		public Thread_Object(string[] args) {
			trd = new Thread(new ParameterizedThreadStart(entry));
			tmp_dir = $"{Path.GetTempPath()}\\{trd.ManagedThreadId}";
			try {
				Directory.CreateDirectory(tmp_dir);
			} catch {
				Console.WriteLine($"Error: Unable to create temporary directory '{Path.GetTempPath()}\\{trd.ManagedThreadId}'.");
				Environment.Exit(0);
			}
			trd.Start(args);
		}

		void entry(object data) {
			Program.arg_mut.WaitOne();
			string[] args = (string[])data;
			parse_args(args);
			Program.arg_mut.ReleaseMutex();

			last_state_path = "last_state\\" + src.Replace(":", "_").Replace("\\", "_").Replace("/", "_") + dst.Replace(":", "_").Replace("\\", "_").Replace("/", "_") + ".txt";
			if(use_ftp) {
				// Build the URI to use for FTP transfer. Example: "ftp://test.com/some/path/".
				// The spec dictates that:
				// 'ftp_srv' is the hostname. Example: "test.com".
				// 'dst' is the substring "ftp:" followed by a filepath using '\' as seperators. Example: "ftp:\some\path\".
				ftp_uri = "ftp://" + ftp_srv + dst.Substring(4).Replace("\\", "/");
				client = new WebClient();
				client.Credentials = new NetworkCredential(ftp_user, ftp_pass);
			}
			watch();
			catch_up();
		}

		void parse_args(string[] args) {
			for(uint i = 0; i < args.Length - 1; i++) {
				if(args[i][0] == '-') {
					if(string.Compare(args[i], "-source") == 0) {
						src = args[++i];
					} else if(string.Compare(args[i], "-destination") == 0) {
						i++;
						if(string.Compare(args[i], 0, "ftp:", 0, 4) == 0) {
							use_ftp = true;
						}
						dst = args[i];
					} else if(string.Compare(args[i], "-ftpserver") == 0) {
						ftp_srv = args[++i];
					} else if(string.Compare(args[i], "-ftpuser") == 0) {
						ftp_user = args[++i];
					} else if(string.Compare(args[i], "-ftppass") == 0) {
						ftp_pass = args[++i];
					} else if(string.Compare(args[i], "-mode") == 0) {
						i++;
						if(string.Compare(args[i], "copy") == 0) mode = Mode.COPY;
						else if(string.Compare(args[i], "resize") == 0) mode = Mode.RESIZE;
						else if(string.Compare(args[i], "thumb") == 0) mode = Mode.THUMB;
						else {
							Console.WriteLine("Invalid mode parameter.");
							Program.exit_with_usage();
						}
					} else if(string.Compare(args[i], "-width") == 0) {
						if(!Int32.TryParse(args[++i], out w)) {
							Console.WriteLine("Invalid width parameter.");
							Program.exit_with_usage();
						}
					} else if(string.Compare(args[i], "-height") == 0) {
						if(!Int32.TryParse(args[++i], out h)) {
							Console.WriteLine("Invalid height parameter.");
							Program.exit_with_usage();
						}
					} else {
						Console.WriteLine($"Invalid parameter identifier: {args[i]}");
						Program.exit_with_usage();
					}
				} else {
					Program.exit_with_usage();
				}
			}
			if(src == null || dst == null) Program.exit_with_usage();
			if(mode == Mode.RESIZE || mode == Mode.THUMB) {
				if(w == 0 || h == 0) {
					Console.WriteLine("Invalid image dimensions.");
					Program.exit_with_usage();
				}
			}
			if(use_ftp) {
				if(ftp_srv == null || ftp_user == null || ftp_pass == null) {
					Console.WriteLine("Invalid FTP credentials.");
					Program.exit_with_usage();
				}
			}
		}

		void watch() {
			watcher = new FileSystemWatcher();
			watcher.Path = src;
			watcher.NotifyFilter = NotifyFilters.LastAccess
								 | NotifyFilters.LastWrite
								 | NotifyFilters.FileName
								 | NotifyFilters.DirectoryName;
			if(use_ftp) {
				if(mode == Mode.COPY) {
					watcher.Created += handle_transfer_ftp;
					watcher.Changed += handle_transfer_ftp;
					watcher.Renamed += handle_transfer_ftp;
				} else if(mode == Mode.RESIZE || mode == Mode.THUMB) {
					watcher.Created += handle_transfer_resize_ftp;
					watcher.Changed += handle_transfer_resize_ftp;
					watcher.Renamed += handle_transfer_resize_ftp;
				}
			} else {
				if(mode == Mode.COPY) {
					watcher.Created += handle_transfer;
					watcher.Changed += handle_transfer;
					watcher.Renamed += handle_transfer;
				} else if(mode == Mode.RESIZE || mode == Mode.THUMB) {
					watcher.Created += handle_transfer_resize;
					watcher.Changed += handle_transfer_resize;
					watcher.Renamed += handle_transfer_resize;
				}
			}
			watcher.EnableRaisingEvents = true;
		}

		void catch_up() {
			string[] src_listing, split_last_state_line, split_src_listing_line;
			string last_state_line, filename_actual, checksum_actual;
			long last_state_overwrite_pos;
			bool found;
			FileStream last_state_fs;
			StreamReader last_state_sr;
			StreamWriter last_state_sw;

			if(File.Exists(last_state_path)) {
				// Open last_state file.
				while(true) {
					try {
						last_state_fs = File.Open(last_state_path, FileMode.OpenOrCreate);
						break;
					} catch {
						Thread.Sleep(1000);
					}
				}
				last_state_sr = new StreamReader(last_state_fs);
				last_state_sw = new StreamWriter(last_state_fs);

				src_listing = Directory.GetFiles(src);

				// For every file actually present in the source directory.
				for(uint i = 0; i < src_listing.Length; i++) {
					split_src_listing_line = src_listing[i].Split('\\');	
					filename_actual = split_src_listing_line[split_src_listing_line.Length - 1];

					found = false;
					do {
						last_state_overwrite_pos = last_state_fs.Position;
						last_state_line =  last_state_sr.ReadLine();
						split_last_state_line = last_state_line.Split('\\');

						// If the file is found in the last_state record!
						if(string.Compare(filename_actual, split_last_state_line[0]) == 0) {
							found = true;
							checksum_actual = file_checksum(src_listing[i]);
							if(string.Compare(checksum_actual, split_last_state_line[1]) != 0) {
								// Checksums are different!

								// Transfer file to destination.
								catch_up_transfer(split_src_listing_line[split_src_listing_line.Length - 1]);

								// Overwrite old record.
								last_state_fs.Position = last_state_overwrite_pos;
								last_state_sw.WriteLine($"{split_src_listing_line[split_src_listing_line.Length - 1]}\\{file_checksum(src_listing[i])}");
								last_state_sw.Flush();
							}

							break;
						}
					} while(last_state_line != null);

					// If the file was not found in the last_state, it must be new.
					if(found == false) {
						// Transfer file to destination.
						catch_up_transfer(split_src_listing_line[split_src_listing_line.Length - 1]);

						// Append new record.
						last_state_fs.Seek(last_state_fs.Length, SeekOrigin.Begin);
						last_state_sw.WriteLine($"{split_src_listing_line[split_src_listing_line.Length - 1]}\\{file_checksum(src_listing[i])}");
						last_state_sw.Flush();
					}

					// Go back to start of last_state file.
					last_state_fs.Seek(0, SeekOrigin.Begin);
					last_state_sr.DiscardBufferedData();
				}

				last_state_fs.Close();
				
			} else {
				if(!Directory.Exists("last_state")) {
					try {
						Directory.CreateDirectory("last_state");
					} catch {
						Console.WriteLine($"Error: Failed to create directory 'last_state'.");
						Environment.Exit(0);
					}
				}
				try {
					// Map out initial state of source directory.
					last_state_fs = File.Create(last_state_path);
					last_state_sw = new StreamWriter(last_state_fs);
					src_listing = Directory.GetFiles(src);

					for(uint i = 0; i < src_listing.Length; i++) {
						// Append new record.
						split_src_listing_line = src_listing[i].Split('\\');
						last_state_fs.Seek(last_state_fs.Length, SeekOrigin.Begin);
						last_state_sw.WriteLine($"{split_src_listing_line[split_src_listing_line.Length - 1]}\\{file_checksum(src_listing[i])}");
					}
					last_state_sw.Flush();
					last_state_fs.Close();
				} catch {
					Console.WriteLine($"Error: Failed to create last state file '{last_state_path}'.");
					Environment.Exit(0);
				}
			}

			// Decrement the counter automically to mark that this thread is done catching up.
			Interlocked.Decrement(ref Program.n_threads_catching_up);
			if(Program.n_threads_catching_up == 0) Console.WriteLine("All transfers have caught up. Server startup complete.");
		}

		void catch_up_transfer(string filename) {
			if(use_ftp) {
				if(mode == Mode.COPY) transfer_ftp(filename);
				else if (mode == Mode.RESIZE || mode == Mode.THUMB) transfer_resize_ftp(filename);
			} else {
				if(mode == Mode.COPY) transfer(filename);
				else if (mode == Mode.RESIZE || mode == Mode.THUMB) transfer_resize(filename);
			}
		}

		string file_checksum(string filepath) {
			FileStream fs;
			string base64_checksum;
			
			while(true) {
				try {
					fs = File.OpenRead(filepath);
					break;
				} catch {
					Thread.Sleep(1000);
				}
			}
			using(var md5 = MD5.Create()) {
				base64_checksum = Convert.ToBase64String(md5.ComputeHash(fs));
			}
			fs.Close();
			return base64_checksum;
		}

		void handle_transfer(object sender, FileSystemEventArgs e) {
			while(Program.n_threads_catching_up != 0) Thread.Sleep(1000);
			transfer(e.Name);
			update_last_state(e.Name);
		}

		void transfer(string filename) {
			while(true) {
				try {
					File.Copy($"{src}\\{filename}", $"{dst}\\{filename}", true);
					break;
				} catch {
					Thread.Sleep(1000);
				}
			}
		}

		void handle_transfer_resize(object sender, FileSystemEventArgs e) {
			while(Program.n_threads_catching_up != 0) Thread.Sleep(1000);
			transfer_resize(e.Name);
			update_last_state(e.Name);
		}

		void transfer_resize(string filename) {
			Image image;
			Bitmap result;
			string[] split_name;
			string modified_name;

			if(mode == Mode.THUMB) {
				split_name = filename.Split('.');
				split_name[0] = split_name[0] + "_thumb";
				modified_name = string.Join(".", split_name);
			} else {
				modified_name = filename;
			}

			while(true) {
				try {
					image = Image.FromFile($"{src}\\{filename}");
					break;
				} catch(OutOfMemoryException) {
					try {
						File.Copy($"{src}\\{filename}", $"{dst}\\{modified_name}", true);
					} catch {}
					return;
				} catch {
					return;
				}
			}
			result = resize(image);
			result.Save($"{dst}\\{modified_name}", image.RawFormat);

			image.Dispose();
		}

		void handle_transfer_ftp(object sender, FileSystemEventArgs e) {
			while(Program.n_threads_catching_up != 0) Thread.Sleep(1000);
			transfer_ftp($"{e.Name}");
			update_last_state(e.Name);
		}

		void transfer_ftp(string filename) {
			while(true) {
				try {
					client.UploadFile($"{ftp_uri}/{filename}", $"{src}\\{filename}");
					break;
				} catch {
					Thread.Sleep(1000);	
				}
			}
		}

		void handle_transfer_resize_ftp(object sender, FileSystemEventArgs e) {
			while(Program.n_threads_catching_up != 0) Thread.Sleep(1000);
			transfer_resize_ftp(e.Name);
			update_last_state(e.Name);
		}

		void transfer_resize_ftp(string filename) {
			Image image;
			Bitmap result;
			string[] split_name;
			string modified_name;

			if(mode == Mode.THUMB) {
				split_name = filename.Split('.');
				split_name[0] = split_name[0] + "_thumb";
				modified_name = string.Join(".", split_name);
			} else {
				modified_name = filename;
			}

			while(true) {
				try {
					image = Image.FromFile($"{src}\\{filename}");
					break;
				} catch(OutOfMemoryException) {
					while(true) {
						try {
							client.UploadFile($"{ftp_uri}/{modified_name}", $"{src}\\{filename}");
							return;
						} catch {
							Thread.Sleep(1000);
						}
					}
				} catch {
					return;
				}
			}
			result = resize(image);
			result.Save($"{tmp_dir}\\{modified_name}", image.RawFormat);
			while(true) {
				try {
					client.UploadFile($"{ftp_uri}/{modified_name}", $"{tmp_dir}\\{modified_name}");
					break;
				} catch {
					Thread.Sleep(1000);
				}
			}
			image.Dispose();
		}

		Bitmap resize(Image image) {
			Rectangle dest_rect;
			Bitmap dest_bmp;
			Graphics graphics;
			ImageAttributes wrap_mode;

			dest_rect = new Rectangle(0, 0, w, h);
			dest_bmp = new Bitmap(w, h);
			dest_bmp.SetResolution(image.HorizontalResolution, image.VerticalResolution); // Set DPI.
			using(graphics = Graphics.FromImage(dest_bmp)) {
				graphics.CompositingMode    = CompositingMode.SourceCopy;
				graphics.CompositingQuality = CompositingQuality.HighQuality;
				graphics.InterpolationMode  = InterpolationMode.HighQualityBicubic;
				graphics.SmoothingMode      = SmoothingMode.HighQuality;
				graphics.PixelOffsetMode    = PixelOffsetMode.HighQuality;
				using(wrap_mode = new ImageAttributes()) {
					wrap_mode.SetWrapMode(WrapMode.TileFlipXY);
					graphics.DrawImage(image, dest_rect, 0, 0, image.Width, image.Height, GraphicsUnit.Pixel, wrap_mode);
				}
			}
			return dest_bmp;
		}

		void update_last_state(string filename) {
			string[] split_line;
			string last_state_line;
			long last_state_overwrite_pos;
			bool found;
			FileStream last_state_fs;
			StreamReader last_state_sr;
			StreamWriter last_state_sw;

			last_state_mut.WaitOne();

			while(true) {
				try {
					last_state_fs = File.Open(last_state_path, FileMode.OpenOrCreate);
					break;
				} catch {
					Thread.Sleep(1000);
				}
			}

			last_state_sr = new StreamReader(last_state_fs);
			last_state_sw = new StreamWriter(last_state_fs);

			found = false;

			do {
				last_state_overwrite_pos = last_state_fs.Position;
				try {
					last_state_line = last_state_sr.ReadLine();
					last_state_fs.Position = last_state_overwrite_pos + last_state_line.Length + 2; // 2 extra characters for "\r\n" line terminator.
					last_state_sr.DiscardBufferedData();
				} catch {
					break;
				}

				split_line = last_state_line.Split('\\');

				if(string.Compare(filename, split_line[0]) == 0) {
					// If the file is found in the last_state record!
					found = true;

					// Overwrite old record.
					last_state_fs.Position = last_state_overwrite_pos;
					last_state_sw.WriteLine($"{filename}\\{file_checksum($"{src}\\{filename}")}");
					last_state_sw.Flush();
					break;
				}

			} while(last_state_line != null);

			if(found == false) {
				// Append new record.
				last_state_fs.Seek(last_state_fs.Length, SeekOrigin.Begin);
				last_state_sw.WriteLine($"{filename}\\{file_checksum($"{src}\\{filename}")}");
				last_state_sw.Flush();
			}

			last_state_fs.Close();
			last_state_mut.ReleaseMutex();
		}
	}
}
