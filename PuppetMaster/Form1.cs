using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class Form1 : Form
    {
        PuppetMaster puppetMaster;
        int pos = 0; // line being run
        public Form1()
        {
            InitializeComponent();
            puppetMaster = new PuppetMaster();
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private void openFileDialog1_FileOk(object sender, CancelEventArgs e)
        {

        }

        private void upload_script_Click(object sender, EventArgs e)
        {
            DialogResult result = openFileDialog1.ShowDialog();
            if (result == DialogResult.OK) // Test result.
            {
                string file = openFileDialog1.FileName;
                int size = -1;
                
                try
                {
                    string text = File.ReadAllText(file);
                    size = text.Length;
                    scriptBox.Text += text;
                    highlight_line(pos);
                    uploadScriptCommands();
                }
                catch (IOException)
                {
                    Console.WriteLine("IO Exception caught");
                }
            }
        }

        private void openFileDialog1_FileOk_1(object sender, CancelEventArgs e)
        {

        }

        private void next_button_Click(object sender, EventArgs e)
        {

            if (pos < scriptBox.Lines.Length - 1)
            {
                puppetMaster.runNextCommand();
                highlight_line(++pos);
                un_highlight_previous_line(pos);
            }
        }

        private void scriptBox_TextChanged(object sender, EventArgs e)
        {
            scriptBox.SelectionStart = scriptBox.Text.Length;
            scriptBox.ScrollToCaret();
        }

        private void textBox1_TextChanged(object sender, EventArgs e)
        {

        }

        private void new_command_button_Click(object sender, EventArgs e)
        {
            puppetMaster.addComand(newCommand.Text);
            if (scriptBox.Lines.Length > 0 && scriptBox.Text[scriptBox.Text.Length - 1] != '\n')
                    scriptBox.Text += "\r\n";
            scriptBox.Text += newCommand.Text + "\r\n";
            newCommand.Text = "";
            highlight_line(pos);
        }

        private void richTextBox1_TextChanged(object sender, EventArgs e)
        {

        }

        private void highlight_line(int position)
        {
            string currentCommand = scriptBox.Lines[position];
            scriptBox.Select(scriptBox.GetFirstCharIndexFromLine(position), currentCommand.Length);
            scriptBox.SelectionColor = Color.Green;
            scriptBox.Select(scriptBox.GetFirstCharIndexFromLine(position), currentCommand.Length);
            scriptBox.SelectionFont = new Font(scriptBox.Font, FontStyle.Bold);
            
        }
        private void un_highlight_previous_line(int position)
        {
            string currentCommand = scriptBox.Lines[position - 1];
            scriptBox.Select(scriptBox.GetFirstCharIndexFromLine(position - 1), currentCommand.Length);
            scriptBox.SelectionColor = Color.Black;
            scriptBox.Select(scriptBox.GetFirstCharIndexFromLine(position - 1), currentCommand.Length);
            scriptBox.SelectionFont = new Font(scriptBox.Font, FontStyle.Regular);
        }

        private void clear_Click(object sender, EventArgs e)
        {
            scriptBox.Text = "";
            pos = 0;
        }

        private void label2_Click(object sender, EventArgs e)
        {
            
        }

        private void cont_button_Click(object sender, EventArgs e)
        {
            puppetMaster.runCommands();
            un_highlight_previous_line(++pos);
            pos = scriptBox.Lines.Length - 1;
        }

        private void label3_Click(object sender, EventArgs e)
        {

        }

        private void uploadScriptCommands()
        {
            foreach (var line in scriptBox.Lines)
            {
                puppetMaster.addComand(line);
            }
        }
    }
}
