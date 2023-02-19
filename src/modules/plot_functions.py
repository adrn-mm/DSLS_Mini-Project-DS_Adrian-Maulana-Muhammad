"""
The purpose of this script is to collect all plot functions for this project
"""

import matplotlib.pyplot as plt


def plot_pie_chart(df, label_column, value_column, title):
    """
    A function to create a pie chart from df
    """
    # Pie chart
    labels = df[label_column].tolist()
    sizes = df[value_column].tolist()

    # colors
    colors = ["#ff9999", "#66b3ff", "#99ff99", "#ffcc99"]

    # plot
    fig1, ax1 = plt.subplots()
    patches, texts = ax1.pie(
        sizes, colors=colors, labels=labels, startangle=90, labeldistance=1.05
    )
    for text in texts:
        text.set_color("grey")

    # Equal aspect ratio ensures that pie is drawn as a circle
    ax1.axis("equal")

    # legend config
    percent_sizes = [(n / sum(sizes) * 100) for n in sizes]
    legend_labels = [f"{l} {s:0.1f}%" for l, s in zip(labels, percent_sizes)]
    plt.legend(bbox_to_anchor=(1.21, 0.5), loc="center", labels=legend_labels)

    # set title
    plt.suptitle(title)

    plt.tight_layout()
    plt.show()


def plot_barh(df, label_column, value_column, axs, x_label, y_label, title, color):
    """
    A function to create a horizontal bar chart
    """
    df.sort_values(value_column, ascending=True, inplace=True)
    df.plot(
        ax=axs,
        kind="barh",
        y=value_column,
        x=label_column,
        title=title,
        fontsize=8,
        xlabel=x_label,
        ylabel=y_label,
        legend=False,
        color=color,
    )


def plot_bar(df, label_column, value_column, axs, x_label, y_label, title, color):
    """
    A function to create a bar chart
    """
    df.plot(
        ax=axs,
        kind="bar",
        y=value_column,
        x=label_column,
        title=title,
        xlabel=x_label,
        ylabel=y_label,
        legend=False,
        color=color,
        rot=0,
        fontsize=8,
    )
